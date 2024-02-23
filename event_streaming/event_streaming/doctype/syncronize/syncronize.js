// Copyright (c) 2019, Frappe Technologies and contributors
// For license information, please see license.txt

frappe.ui.form.on("Syncronize", {
	refresh: function (frm) {
		
			frm.add_custom_button(__("Resync"), function () {
			//	setInterval(function () {
				
			
				frappe.call({
					method: "event_streaming.event_streaming.doctype.event_producer.event_producer.pull_from_node_x",
					args: {
						event_producer: frm.doc.producer,
					},
					callback: function (r) {
						if (r.message) {
							frappe.msgprint(r.message);
							frm.set_value("status", r.message);
							frm.save();
						}
					},
				});	
			//}, 60000); 
			});
			frm.add_custom_button(__("Send"), function () {
				frappe.call({
					method: "event_streaming.event_streaming.doctype.event_producer.event_producer_send.send_to_node",
					args: {
						event_producer: frm.doc.producer,
						event_consumer: frm.doc.consumer,
					},
					callback: function (r) {
						if (r.message) {
							frappe.msgprint(r.message);
							frm.set_value("status", r.message);
							frm.save();
						}
					},
				});
			});
		}
	
});
