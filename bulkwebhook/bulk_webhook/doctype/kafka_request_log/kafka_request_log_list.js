if (!frappe.listview_settings["Kafka Request Log"]) {
    frappe.listview_settings["Kafka Request Log"] = {};
}

frappe.listview_settings['Kafka Request Log'].refresh = async function (list) {
    if (!frappe.user.has_role("System Manager")) return;

    list.page.clear_inner_toolbar();

    const logging_enabled = await frappe.xcall("bulkwebhook.bulk_webhook.doctype.kafka_request_log.kafka_request_log.is_logging_enabled");
    list.page.add_inner_button(__(logging_enabled ? "Disable Logging" : "Enable Logging"), () => {
        frappe.xcall("bulkwebhook.bulk_webhook.doctype.kafka_request_log.kafka_request_log.toggle_logging", {
                "enable": !logging_enabled
            })
            .then(() => {
                this.refresh(list);
            });
    });
}
