# Copyright (c) 2022, Aakvatech and contributors
# For license information, please see license.txt

from __future__ import unicode_literals
import json
import frappe
from frappe import _
from frappe.model.document import Document
from frappe.utils.jinja import validate_template
from bulkwebhook.bulk_webhook.doctype.kafka_settings.kafka_utlis import send_kafka
from bulkwebhook.bulk_webhook.doctype.bulk_webhook.bulk_webhook import log_request
from frappe.integrations.doctype.webhook.webhook import get_context as get_webhook_context

WEBHOOK_CONTEXT = get_webhook_context(doc=None)


class KafkaHook(Document):
    def validate(self):
        self.validate_docevent()
        self.validate_condition()
        self.validate_request_body()

    def on_update(self):
        frappe.cache().delete_value("kafkahook")

    def on_trash(self):
        frappe.cache().delete_value("kafkahook")

    def validate_docevent(self):
        if self.webhook_doctype:
            is_submittable = frappe.get_value(
                "DocType", self.webhook_doctype, "is_submittable"
            )
            if not is_submittable and self.webhook_docevent in [
                "on_submit",
                "on_cancel",
                "on_update_after_submit",
            ]:
                frappe.throw(
                    _("DocType must be Submittable for the selected Doc Event")
                )

    def validate_condition(self):
        temp_doc = frappe.new_doc(self.webhook_doctype)
        if self.condition:
            try:
                frappe.safe_eval(self.condition, eval_locals={
                                 **WEBHOOK_CONTEXT, "doc": temp_doc})
            except Exception as e:
                frappe.throw(_(e))

    def validate_request_body(self):
        validate_template(self.webhook_json)


def enqueue_webhook(doc, kafka_hook):
    hook = frappe.get_doc("Kafka Hook", kafka_hook.get("name"))
    data = get_webhook_data(doc, hook)
    try:
        r = send_kafka(
            hook.kafka_settings,
            hook.kafka_topic,
            None,
            data,
        )
        log_request(hook.kafka_topic, hook.kafka_settings, data, str(r))

    except Exception as e:
        frappe.log_error(str(e))
        log_request(
            "Error: " + hook.kafka_topic,
            hook.kafka_settings,
            data,
            r,
        )


def get_webhook_data(doc, kafka_hook):
    data = {}
    doc = doc.as_dict(convert_dates_to_str=True)
    data = frappe.render_template(kafka_hook.webhook_json, context={
                                  **WEBHOOK_CONTEXT, "doc": doc})
    data = json.loads(data)
    return data


def run_webhooks(doc, method):
    """Run webhooks for this method"""
    if (
        frappe.flags.in_import
        or frappe.flags.in_patch
        or frappe.flags.in_install
        or frappe.flags.in_migrate
    ):
        return

    if frappe.flags.kafkahook_executed is None:
        frappe.flags.kafkahook_executed = {}

    if frappe.flags.kafkahook is None:
        # load webhooks from cache
        webhooks = frappe.cache().get_value("kafkahook")
        if webhooks is None:
            # query webhooks
            webhooks_list = frappe.get_all(
                "Kafka Hook",
                fields=["name", "`condition`",
                        "webhook_docevent", "webhook_doctype"],
                filters={"enabled": True},
            )
            # make webhooks map for cache
            webhooks = {}
            for w in webhooks_list:
                webhooks.setdefault(w.webhook_doctype, []).append(w)
            frappe.cache().set_value("kafkahook", webhooks)

        frappe.flags.kafkahook = webhooks

    # get webhooks for this doctype
    webhooks_for_doc = frappe.flags.kafkahook.get(doc.doctype, None)

    if not webhooks_for_doc:
        # no webhooks, quit
        return

    def _webhook_request(webhook):
        if webhook.name not in frappe.flags.kafkahook_executed.get(doc.name, []):
            frappe.enqueue(
                "bulkwebhook.bulk_webhook.doctype.kafka_hook.kafka_hook.enqueue_webhook",
                enqueue_after_commit=True,
                doc=doc,
                kafka_hook=webhook,
            )

            # keep list of webhooks executed for this doc in this request
            # so that we don't run the same webhook for the same document multiple times
            # in one request
            frappe.flags.kafkahook_executed.setdefault(doc.name, []).append(
                webhook.name
            )

    event_list = ["on_update", "after_insert",
                  "on_submit", "on_cancel", "on_trash"]

    if not doc.flags.in_insert:
        # value change is not applicable in insert
        event_list.append("on_change")
        event_list.append("before_update_after_submit")

    from frappe.integrations.doctype.webhook.webhook import get_context

    for webhook in webhooks_for_doc:
        trigger_webhook = False
        event = method if method in event_list else None
        if not webhook.condition:
            trigger_webhook = True
        elif frappe.safe_eval(webhook.condition, eval_locals={**WEBHOOK_CONTEXT, "doc": doc}):
            trigger_webhook = True

        if trigger_webhook and event and webhook.webhook_docevent == event:
            _webhook_request(webhook)
