# Copyright (c) 2021, Aakvatech and contributors
# For license information, please see license.txt

from __future__ import unicode_literals
from datetime import timedelta
import datetime
import requests
import base64
import hashlib
import hmac
import json
from time import sleep
import frappe
from frappe import _
from frappe.model.document import Document
from frappe.utils.background_jobs import enqueue
from frappe.utils import (
    now_datetime,
    today,
    add_to_date,
)
from frappe.utils.jinja import validate_template
from frappe.utils.safe_exec import get_safe_globals, NamespaceDict, safe_exec
from types import FunctionType, MethodType, ModuleType
from bulkwebhook.bulk_webhook.doctype.kafka_settings.kafka_utlis import send_kafka


WEBHOOK_SECRET_HEADER = "X-Frappe-Webhook-Signature"


class BulkWebhook(Document):
    def validate(self):
        self.validate_mandatory_fields()
        self.validate_request_body()

    def validate_request_body(self):
        if not self.source == "Report":
            return
        if self.request_structure:
            if self.request_structure == "Form URL-Encoded":
                self.webhook_json = None
            elif self.request_structure == "JSON":
                validate_template(self.webhook_json)
                self.webhook_data = []

    def validate_mandatory_fields(self):
        # Check if all Mandatory Report Filters are filled by the User
        filters = frappe.parse_json(self.filters) if self.filters else {}
        filter_meta = frappe.parse_json(self.filter_meta) if self.filter_meta else {}
        throw_list = []
        for meta in filter_meta:
            if meta.get("reqd") and not filters.get(meta["fieldname"]):
                throw_list.append(meta["label"])
        if throw_list:
            frappe.throw(
                title=_("Missing Filters Required"),
                msg=_("Following Report Filters have missing values:")
                + "<br><br><ul><li>"
                + " <li>".join(throw_list)
                + "</ul>",
            )

    def get_script_data(self):
        exec_globals, _locals = safe_exec(self.script, _locals={})
        data = _locals.get(self.script_return_variable)
        return data

    def get_method_data(self, get_method_data=None):
        kwargs = get_method_data
        if not kwargs:
            kwargs = json.loads(self.method_parameters)
        data = frappe.get_attr(self.method)(**kwargs)
        return data

    def get_report_data(self, report_filters=None):
        """Returns file in for the report in given format"""
        report = frappe.get_doc("Report", self.report)
        self.filters = report_filters
        if not self.filters:
            self.filters = frappe.parse_json(self.filters) if self.filters else {}

        if self.report_type == "Report Builder" and self.data_modified_till:
            self.filters["modified"] = (
                ">",
                now_datetime() - timedelta(hours=self.data_modified_till),
            )

        if self.report_type != "Report Builder" and self.dynamic_date_filters_set():
            self.prepare_dynamic_filters()

        columns, data = report.get_data(
            user=self.user,
            filters=self.filters,
            as_dict=True,
            ignore_prepared_report=True,
        )

        # add serial numbers
        columns.insert(0, frappe._dict(fieldname="idx", label="", width="30px"))
        for i in range(len(data)):
            data[i]["idx"] = i + 1

        if len(data) == 0 and self.send_if_data:
            return None

        return data

    def prepare_dynamic_filters(self):
        self.filters = frappe.parse_json(self.filters)

        to_date = today()
        from_date_value = {
            "Daily": ("days", -1),
            "Weekly": ("weeks", -1),
            "Monthly": ("months", -1),
            "Quarterly": ("months", -3),
            "Half Yearly": ("months", -6),
            "Yearly": ("years", -1),
        }[self.dynamic_date_period]

        from_date = add_to_date(to_date, **{from_date_value[0]: from_date_value[1]})

        self.filters[self.from_date_field] = from_date
        self.filters[self.to_date_field] = to_date

    def send(self):
        if self.filter_meta and not self.filters:
            frappe.throw(_("Please set filters value in Report Filter table."))

        data_list = get_webhook_data(self)

        if not data_list or len(data_list) == 0:
            frappe.msgprint(_("No Data to send"))
            return

        enqueue(
            method=enqueue_bulk_webhook,
            queue="long",
            timeout=10000,
            is_async=True,
            bulk_webhook_name=self.name,
            job_name="Bulk Webhook: " + self.title,
        )

    def dynamic_date_filters_set(self):
        return self.dynamic_date_period and self.from_date_field and self.to_date_field


@frappe.whitelist()
def send_now(name):
    """Send Auto Email report now"""
    webhook = frappe.get_cached_doc("Bulk Webhook", name)
    webhook.check_permission()
    webhook.send()


# Webhook
def get_context(data):
    return {"data": data, "utils": get_safe_globals().get("frappe").get("utils")}


def enqueue_bulk_webhook(
    bulk_webhook_name, method_parameters=None, report_filters=None
):
    webhook = frappe.get_cached_doc("Bulk Webhook", bulk_webhook_name)
    headers = get_webhook_headers(webhook)
    data_list = get_webhook_data(webhook, method_parameters, report_filters)
    if not data_list or len(data_list) == 0:
        return
    url = webhook.request_url

    if not url:
        url = frappe.get_cached_value("Bulk Webhook Settings", "Bulk Webhook Settings", "url")
    if webhook.request_type == "API":
        for data_row in data_list:
            for i in range(3):
                r = {}
                try:
                    r = requests.request(
                        method=webhook.request_method,
                        url=url,
                        data=json.dumps(data_row[1], default=str),
                        headers=headers,
                        timeout=5,
                    )
                    r.raise_for_status()
                    frappe.logger().debug({"webhook_success": r.text})
                    log_request(url, headers, data_row[1], r.json())
                    return r.json()
                except Exception as e:
                    frappe.logger().debug({"webhook_error": e, "try": i + 1})
                    log_request(url, headers, data_row[1], r.json())
                    sleep(3 * i + 1)
                    if i != 2:
                        continue
                    else:
                        raise e
    elif webhook.request_type == "Kafka":
        for data_row in data_list:
            r = {}
            try:
                r = send_kafka(
                    webhook.kafka_settings,
                    webhook.kafka_topic,
                    data_row[0],
                    data_row[1],
                )
                log_request(
                    webhook.kafka_topic, webhook.kafka_settings, data_row[1], str(r)
                )
                return str(r)

            except Exception as e:
                frappe.log_error(
                    frappe.get_traceback(), str(webhook.title + ": " + str(e))[0:140]
                )
                log_request(
                    "Error: " + webhook.kafka_topic,
                    webhook.kafka_settings,
                    data_row[1],
                    r,
                )


def enqueue_bulk_webhooks(frequency):
    webhooks = frappe.get_all(
        "Bulk Webhook",
        filters={"enabled": 1, "frequency": frequency},
        fields=["name", "title"],
    )
    for webhook in webhooks:
        enqueue(
            method=enqueue_bulk_webhook,
            queue="long",
            timeout=10000,
            is_async=True,
            bulk_webhook_name=webhook.name,
            job_name="Bulk Webhook: " + webhook.title,
        )


def log_request(url, headers, data, res):
    request_log = frappe.get_doc(
        {
            "doctype": "Webhook Request Log",
            "user": frappe.session.user if frappe.session.user else None,
            "url": url,
            "headers": json.dumps(headers, indent=4) if headers else None,
            "data": json.dumps(data, indent=4) if isinstance(data, dict) else data,
            "response": json.dumps(res, indent=4) if res else None,
        }
    )

    request_log.insert(ignore_permissions=True)
    frappe.db.commit()


def get_webhook_headers(webhook):
    headers = {}
    if webhook.enable_security:
        data = get_webhook_data(webhook)
        signature = base64.b64encode(
            hmac.new(
                webhook.get_password("webhook_secret").encode("utf8"),
                json.dumps(data).encode("utf8"),
                hashlib.sha256,
            ).digest()
        )
        headers[WEBHOOK_SECRET_HEADER] = signature

    if webhook.webhook_headers:
        for h in webhook.webhook_headers:
            if h.get("key") and h.get("value"):
                headers[h.get("key")] = h.get("value")
    else:
        settings = frappe.get_single("Bulk Webhook Settings")
        if settings.headers:
            for h in settings.headers:
                if h.get("key") and h.get("value"):
                    headers[h.get("key")] = h.get("value")

    return headers


def get_webhook_data(webhook, method_parameters=None, report_filters=None):
    data = {}
    if webhook.source == "Report":
        _data = webhook.get_report_data(report_filters)
    elif webhook.source == "Method":
        _data = webhook.get_method_data(method_parameters)
    elif webhook.source == "Script":
        _data = webhook.get_script_data()
    if not _data:
        return
    group_dict = {}
    data_list = []
    for rec in _data:
        copy_rec = rec.copy()
        for key, value in rec.items():
            # Convert datetime object to string
            if isinstance(
                value,
                (datetime.datetime, datetime.time, datetime.date, datetime.timedelta),
            ):
                copy_rec[key] = str(value)
        if webhook.group_by:
            if rec.get(webhook.group_by):
                group_dict.setdefault(rec.get(webhook.group_by), [])
                group_dict[rec.get(webhook.group_by)].append(copy_rec)
        else:
            group_dict.setdefault("None", [])
            group_dict["None"].append(copy_rec)

    for key, value in group_dict.items():
        data = None
        if webhook.webhook_json:
            data = frappe.render_template(webhook.webhook_json, get_context(value))
            data = json.loads(data)
        else:
            data = json.loads(value)
        data_list.append([key, data])

    return data_list


@frappe.whitelist()
def get_autocompletion_items():
    """Generates a list of a autocompletion strings from the context dict
    that is used while executing a Server Script.

    Returns:
        list: Returns list of autocompletion items.
        For e.g., ["frappe.utils.cint", "frappe.db.get_all", ...]
    """

    def get_keys(obj):
        out = []
        for key in obj:
            if key.startswith("_"):
                continue
            value = obj[key]
            if isinstance(value, (NamespaceDict, dict)) and value:
                if key == "form_dict":
                    out.append(["form_dict", 7])
                    continue
                for subkey, score in get_keys(value):
                    fullkey = f"{key}.{subkey}"
                    out.append([fullkey, score])
            else:
                if isinstance(value, type) and issubclass(value, Exception):
                    score = 0
                elif isinstance(value, ModuleType):
                    score = 10
                elif isinstance(value, (FunctionType, MethodType)):
                    score = 9
                elif isinstance(value, type):
                    score = 8
                elif isinstance(value, dict):
                    score = 7
                else:
                    score = 6
                out.append([key, score])
        return out

    items = frappe.cache().get_value("server_script_autocompletion_items")
    if not items:
        items = get_keys(get_safe_globals())
        items = [{"value": d[0], "score": d[1]} for d in items]
        frappe.cache().set_value("server_script_autocompletion_items", items)
    return items
