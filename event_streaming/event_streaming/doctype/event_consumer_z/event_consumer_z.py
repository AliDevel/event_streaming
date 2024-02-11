# Copyright (c) 2024, Frappe Technologies and contributors
# For license information, please see license.txt

# import frappe
from frappe.model.document import Document
import frappe
from frappe import _
from frappe.custom.doctype.custom_field.custom_field import create_custom_field
from frappe.frappeclient import FrappeClient
from frappe.model.document import Document
from frappe.utils.background_jobs import get_jobs
from frappe.utils.data import get_link_to_form, get_url
from frappe.utils.password import get_decrypted_password
class EventConsumerZ(Document):
	def set_last_update(self, last_update):
		last_update_doc_name = frappe.db.get_value(
			"Event Consumer Z Last Update", dict(event_consumer_z=self.name)
		)
		if not last_update_doc_name:
			frappe.get_doc(
				dict(
					doctype="Event Consumer Z Last Update",
					event_consumer_z=self.name,
					last_update=last_update,
				)
			).insert(ignore_permissions=True)
		else:
			frappe.db.set_value(
				"Event Consumer Z Last Update", last_update_doc_name, "last_update", last_update
			)
	
	def get_last_update(self):
		return frappe.db.get_value(
			"Event Consumer Z Last Update", dict(event_consumer_z = self.name), "last_update"
		)
