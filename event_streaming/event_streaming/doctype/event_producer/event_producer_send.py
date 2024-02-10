# Copyright (c) 2019, Frappe Technologies and contributors
# License: MIT. See LICENSE

import json
import time

import requests
from datetime import datetime
import frappe
from frappe import _
from frappe.custom.doctype.custom_field.custom_field import create_custom_field
from frappe.frappeclient import FrappeClient
from frappe.model.document import Document
from frappe.utils.background_jobs import get_jobs
from frappe.utils.data import get_link_to_form, get_url
from frappe.utils.password import get_decrypted_password




def get_producer_site(producer_url):
	"""create a FrappeClient object for event producer site"""
	producer_doc = frappe.get_doc("Event Producer", producer_url)
	producer_site = FrappeClient(
		url=producer_url,
		api_key=producer_doc.api_key,
		api_secret=producer_doc.get_password("api_secret"),
	)
	return producer_site
def get_consumer_site(consumer_url):
	"""create a FrappeClient object for event producer site"""
	producer_doc = frappe.get_doc("Event Consumer Z", consumer_url)
	producer_site = FrappeClient(
		url=consumer_url,
		api_key=producer_doc.api_key,
		api_secret=producer_doc.get_password("api_secret"),
	)
	return producer_site


def get_approval_status(config, ref_doctype):
	"""check the approval status for consumption"""
	for entry in config:
		if entry.get("ref_doctype") == ref_doctype:
			return entry.get("status")
	return "Pending"


@frappe.whitelist()
def pull_producer_data(update, event_producer, in_retry=False):
    """Sync the individual update"""
    #frappe.log_error(frappe.get_traceback(), 'payment failed')

    if isinstance(update, str):
        update = frappe.parse_json(update)
        frappe.log_error(frappe.get_traceback(), frappe.parse_json(update))

    event_producer = event_producer# event_producer
   ## frappe.log_error(frappe.get_traceback(), 'Hi')

    try:
        if update.update_type == "Create":
            set_insert(update, event_producer)
        elif update.update_type == "Update":
            set_update(update)
        elif update.update_type == "Delete":
            set_delete(update)

        if in_retry:
            return "Synced"
        
        log_event_sync(update, event_producer, "Synced")  
    except Exception:
        if in_retry:
            if frappe.flags.in_test:
                print(frappe.get_traceback())
            return "Failed"
        log_event_sync(update, event_producer, "Failed", frappe.get_traceback())

    frappe.db.commit()



@frappe.whitelist()
def send_to_node(event_producer, event_consumer):
    """Pull all updates after the last update timestamp from the event producer site.
    In this case, it will send from the local site to the Remote Site"""
    event_producer = frappe.get_doc("Event Producer", event_producer)
    producer_site = event_producer.producer_url

    event_consumer_doc = frappe.get_doc("Event Consumer Z", event_consumer)
    consumer_site = get_consumer_site(event_consumer_doc.callback_url)

    last_update = '0'  # event_consumer_doc.get_last_update()
    if 'T' in last_update:
        last_update = datetime.strptime(last_update, "%Y-%m-%dT%H:%M:%S.%f")
        last_update = last_update.strftime("%Y-%m-%d %H:%M:%S.%f")
    frappe.msgprint(str(last_update))

    (doctypes, mapping_config, naming_config) = get_config(event_producer.producer_doctypes)

    updates = get_updates(event_consumer_doc.callback_url, last_update, doctypes)
    frappe.msgprint(str(updates))

    for update in updates:
        update.use_same_name = naming_config.get(update.ref_doctype)
        mapping = mapping_config.get(update.ref_doctype)
        update.creation = update.creation.isoformat()

        if mapping:
            update.mapping = mapping
            update = get_mapped_update(update, producer_site)
        if not update.update_type == "Delete":
            update.data = update.data
            frappe.msgprint(str(update.data))  # Fixed indentation
            # Construct a list of JSON-formatted strings

            x = consumer_site.post_request(
                {
                    "cmd": "event_streaming.event_streaming.doctype.event_producer.event_producer_send.pull_producer_data",
                    "update": frappe.parse_json(update),
                    "event_producer": producer_site,
                }
            )

            # event_consumer_doc.set_last_update(update.creation)

    return last_update

@frappe.whitelist()
def create():
	doc = {
 "__unsaved": 1,
 "additional_discount_percentage": 0.0,
 "address_display": "Nur bazar t\u00f6weregi<br>Da\u015foguz<br>\nTurkmenistan<br>\n",
 "advances": [],
 "against_income_account": "\u041f\u0440\u043e\u0434\u0430\u0436\u0438 - C",
 "allocate_advances_automatically": 0,
 "amount_eligible_for_commission": 0.0,
 "apply_discount_on": "Grand Total",
 "base_change_amount": 0.0,
 "base_discount_amount": 0.0,
 "base_grand_total": 0.0,
 "base_in_words": "TMM Zero only.",
 "base_net_total": 0.0,
 "base_paid_amount": 0.0,
 "base_rounded_total": 0.0,
 "base_rounding_adjustment": 0.0,
 "base_total": 0.0,
 "base_total_taxes_and_charges": 0.0,
 "base_write_off_amount": 0.0,
 "change_amount": 0.0,
 "commission_rate": 0.0,
 "company": "5Dogan",
 "contact_display": "Abadan market",
 "contact_email": "",
 "contact_mobile": "8.65.03.33.88.",
 "contact_person": "Abadan markett-Abadan markett",
 "conversion_rate": 1.0,
 "creation": "2024-02-11 00:06:49.448007",
 "currency": "TMM",
 "customer": "Abadan market",
 "customer_address": "nur bazar-\u0412\u044b\u0441\u0442\u0430\u0432\u043b\u0435\u043d\u0438\u0435 \u0441\u0447\u0435\u0442\u043e\u0432",
 "customer_group": "\u041a\u043e\u043c\u043c\u0435\u0440\u0447\u0435\u0441\u043a\u0438\u0439 \u0441\u0435\u043a\u0442\u043e\u0440",
 "customer_name": "Abadan market",
 "debit_to": "\u0414\u0435\u0431\u0435\u0442\u043e\u0440\u044b - C",
 "disable_rounded_total": 1,
 "discount_amount": 0.0,
 "docstatus": 0,
 "doctype": "Sales Invoice",
 "dont_create_loyalty_points": 0,
 "due_date": "2024-02-18",
 "grand_total": 0.0,
 "group_same_items": 0,
 "idx": 0,
 "ignore_default_payment_terms_template": 0,
 "ignore_pricing_rule": 0,
 "in_words": "TMM Zero only.",
 "is_cash_or_non_trade_discount": 0,
 "is_consolidated": 0,
 "is_debit_note": 0,
 "is_discounted": 0,
 "is_internal_customer": 0,
 "is_opening": "No",
 "is_pos": 0,
 "is_return": 0,
 "items": [
  {
   "__unsaved": 1,
   "actual_batch_qty": 0.0,
   "actual_qty": 22.11,
   "allow_zero_valuation_rate": 0,
   "amount": 0.0,
   "barcode": "000001",
   "base_amount": 0.0,
   "base_net_amount": 0.0,
   "base_net_rate": 0.0,
   "base_price_list_rate": 0.0,
   "base_rate": 0.0,
   "base_rate_with_margin": 0.0,
   "conversion_factor": 1.0,
   "cost_center": "\u041e\u0441\u043d\u043e\u0432\u043d\u044b\u0435 - C",
   "creation": "2024-02-11 00:06:49.448007",
   "delivered_by_supplier": 0,
   "delivered_qty": 0.0,
   "description": "Koke pay taze ay pechine",
   "discount_account": "\u041c\u0430\u0440\u043a\u0435\u0442\u0438\u043d\u0433\u043e\u0432\u044b\u0435 \u0440\u0430\u0441\u0445\u043e\u0434\u044b - C",
   "discount_amount": 0.0,
   "discount_percentage": 0.0,
   "docstatus": 0,
   "doctype": "Sales Invoice Item",
   "enable_deferred_revenue": 0,
   "expense_account": "\u0421\u0435\u0431\u0435\u0441\u0442\u043e\u0438\u043c\u043e\u0441\u0442\u044c \u043f\u0440\u043e\u0434\u0430\u043d\u043d\u044b\u0445 \u043f\u0440\u043e\u0434\u0443\u043a\u0442\u043e\u0432 - C",
   "grant_commission": 1,
   "has_item_scanned": 0,
   "idx": 1,
   "image": "",
   "income_account": "\u041f\u0440\u043e\u0434\u0430\u0436\u0438 - C",
   "incoming_rate": 38.0,
   "is_fixed_asset": 0,
   "is_free_item": 0,
   "item_code": "000001",
   "item_group": "\u0420\u0430\u0437\u0432\u0435\u0441\u043d\u044b\u0435 \u0442\u043e\u0432\u0430\u0440\u044b",
   "item_name": "Koke pay taze ay pechine",
   "item_tax_rate": "{}",
   "margin_rate_or_amount": 0.0,
   "margin_type": "",
   "modified": "2024-02-11 00:06:49.448007",
   "modified_by": "Administrator",
   "name": "ba0d9b7dd2",
   "net_amount": 0.0,
   "net_rate": 0.0,
   "owner": "Administrator",
   "page_break": 0,
   "parent": "ss000000000000001",
   "parentfield": "items",
   "parenttype": "Sales Invoice",
   "posa_is_offer": 0,
   "posa_offer_applied": 0,
   "price_list_rate": 0.0,
   "pricing_rules": "",
   "qty": 1.0,
   "rate": 0.0,
   "rate_with_margin": 0.0,
   "stock_qty": 1.0,
   "stock_uom": "\u043a\u0433",
   "stock_uom_rate": 0.0,
   "total_weight": 0.0,
   "uom": "\u043a\u0433",
   "warehouse": "\u041c\u0430\u0433\u0430\u0437\u0438\u043d\u044b - C",
   "weight_per_unit": 0.0
  }
 ],
 "language": "ru",
 "letter_head": "Default",
 "loyalty_amount": 0.0,
 "loyalty_points": 0,
 "modified": "2024-02-11 00:06:49.448007",
 "modified_by": "Administrator",
 "name": "ss000000000000001",
 "naming_series": "ACC-SINV-.YYYY.-",
 "net_total": 0.0,
 "only_include_allocated_payments": 0,
 "outstanding_amount": 0.0,
 "owner": "Administrator",
 "packed_items": [],
 "paid_amount": 0.0,
 "party_account_currency": "TMM",
 "payment_schedule": [
  {
   "__unsaved": 1,
   "base_payment_amount": 0.0,
   "creation": "2024-02-11 00:06:49.448007",
   "description": "7 Gundan son Polny tolag atmali",
   "discount": 0.0,
   "discount_date": "2024-02-11",
   "discount_type": "Percentage",
   "discounted_amount": 0.0,
   "docstatus": 0,
   "doctype": "Payment Schedule",
   "due_date": "2024-02-18",
   "idx": 1,
   "invoice_portion": 100.0,
   "modified": "2024-02-11 00:06:49.448007",
   "modified_by": "Administrator",
   "name": "9e0d44563c",
   "outstanding": 0.0,
   "owner": "Administrator",
   "paid_amount": 0.0,
   "parent": "ss000000000000001",
   "parentfield": "payment_schedule",
   "parenttype": "Sales Invoice",
   "payment_amount": 0.0,
   "payment_term": "7Gun"
  }
 ],
 "payment_terms_template": "7 Gun",
 "payments": [],
 "plc_conversion_rate": 1.0,
 "po_no": "",
 "posa_coupons": [],
 "posa_delivery_charges_rate": 0.0,
 "posa_is_printed": 0,
 "posa_offers": [],
 "posting_date": "2024-02-11",
 "posting_time": "00:06:49.866949",
 "price_list_currency": "TMM",
 "pricing_rules": [],
 "redeem_loyalty_points": 0,
 "remarks": "No Remarks",
 "repost_required": 0,
 "rounded_total": 0.0,
 "rounding_adjustment": 0.0,
 "sales_team": [],
 "selling_price_list": "Pra\u00fds.Zakaz",
 "set_posting_time": 0,
 "status": "Draft",
 "tax_category": "",
 "tax_id": "",
 "taxes": [],
 "territory": "Turkmenistan",
 "timesheets": [],
 "title": "Abadan market",
 "total": 0.0,
 "total_advance": 0.0,
 "total_billing_amount": 0.0,
 "total_billing_hours": 0.0,
 "total_commission": 0.0,
 "total_net_weight": 0.0,
 "total_qty": 1.0,
 "total_taxes_and_charges": 0.0,
 "update_billed_amount_in_delivery_note": 1,
 "update_billed_amount_in_sales_order": 0,
 "update_stock": 0,
 "use_company_roundoff_cost_center": 0,
 "workflow_state": "\u0417\u0430\u043a\u0430\u0437 \u043f\u043e\u043b\u0443\u0447\u0435\u043d",
 "write_off_amount": 0.0,
 "write_off_outstanding_amount_automatically": 0
}
	doc1 = frappe.get_doc(doc)
	doc1.insert()
	frappe.db.commit()
	return doc1

@frappe.whitelist()
def convert_to_serializable(obj):
    if isinstance(obj, datetime):
        return obj.strftime('%Y-%m-%d %H:%M:%S.%f')
    else:
        raise TypeError("Object of type {} is not serializable".format(type(obj)))

				

	

def get_config(event_config):
    """get the doctype mapping and naming configurations for consumption"""
    doctypes, mapping_config, naming_config = [], {}, {}

    for entry in event_config:
       
        if entry.status != "A":
            if entry.has_mapping:
                (mapped_doctype, mapping) = frappe.db.get_value(
                    "Document Type Mapping", entry.mapping, ["remote_doctype", "name"]
                )
                mapping_config[mapped_doctype] = mapping
                naming_config[mapped_doctype] = entry.use_same_name
                doctypes.append(mapped_doctype)
            else:
                naming_config[entry.ref_doctype] = entry.use_same_name
                doctypes.append(entry.ref_doctype)
    return (doctypes, mapping_config, naming_config)




def set_insert(update,  event_producer):
	"""Sync insert type update"""

	#frappe.log_error(frappe.get_traceback(), 'insert1')	
	#frappe.log_error(frappe.get_traceback(), str(update.data))
	doc = frappe.get_doc(update.data)
	#frappe.log_error(frappe.get_traceback(), 'insert2')	
	
	#if update.use_same_name:
	doc.insert(set_name=update.docname, set_child_names=False)
	frappe.db.commit()

	#else:
		# if event consumer is not saving documents with the same name as the producer
		# store the remote docname in a custom field for future updates
		#doc.remote_docname = update.docname
		#doc.remote_site_name = event_producer
		#doc.insert(set_child_names=False)


def set_update(update):
    """Sync update type update"""
    local_doc = get_local_doc(update)
   
    if local_doc:
        data = frappe._dict(update.data)

        if data.changed:
            local_doc.update(data.changed)
        if data.removed:
            local_doc = update_row_removed(local_doc, data.removed)
        if data.row_changed:
            update_row_changed(local_doc, data.row_changed)
        if data.added:
            local_doc = update_row_added(local_doc, data.added)

        local_doc.save()
        local_doc.db_update_all()


def update_row_removed(local_doc, removed):
	"""Sync child table row deletion type update"""
	for tablename, rownames in removed.items():
		table = local_doc.get_table_field_doctype(tablename)
		for row in rownames:
			table_rows = local_doc.get(tablename)
			child_table_row = get_child_table_row(table_rows, row)
			table_rows.remove(child_table_row)
			local_doc.set(tablename, table_rows)
	return local_doc


def get_child_table_row(table_rows, row):
	for entry in table_rows:
		if entry.get("name") == row:
			return entry


def update_row_changed(local_doc, changed):
	"""Sync child table row updation type update"""
	for tablename, rows in changed.items():
		old = local_doc.get(tablename)
		for doc in old:
			for row in rows:
				if row["name"] == doc.get("name"):
					doc.update(row)


def update_row_added(local_doc, added):
	"""Sync child table row addition type update"""
	for tablename, rows in added.items():
		local_doc.extend(tablename, rows)
		for child in rows:
			child_doc = frappe.get_doc(child)
			child_doc.parent = local_doc.name
			child_doc.parenttype = local_doc.doctype
			child_doc.insert(set_name=child_doc.name)
	return local_doc


def set_delete(update):
	"""Sync delete type update"""
	local_doc = get_local_doc(update)
	if local_doc:
		local_doc.delete()


def get_updates(consumer_site, last_update, doctypes):
	"""Get all updates generated after the last update timestamp"""
	docs = get_update_logs_for_consumer(consumer_site,doctypes,last_update)
	
	return [frappe._dict(d) for d in (docs or [])]

@frappe.whitelist()
def get_update_logs_for_consumer(event_consumer, doctypes, last_update):
    """
    Fetches all the UpdateLogs for the consumer
    It will inject old un-consumed Update Logs if a doc was just found to be accessible to the Consumer
    """

    if isinstance(doctypes, str):
        doctypes = frappe.parse_json(doctypes)

    from event_streaming.event_streaming.doctype.event_consumer.event_consumer import has_consumer_access

    consumer = frappe.get_doc("Event Consumer Z", event_consumer)
    docs = frappe.get_list(
        doctype="Event Update Log",
        filters={"ref_doctype": ("in", doctypes), "creation": (">", last_update)},
        fields=["update_type", "ref_doctype", "docname", "data", "name","creation"],
        order_by="creation desc",
    )
	
    frappe.msgprint(str(len(docs)))
    result = []
    to_update_history = []
    for d in docs:
        if (d.ref_doctype, d.docname) in to_update_history:
            # will be notified by background jobs
            continue

     

        if not is_consumer_uptodate(d, consumer):
            to_update_history.append((d.ref_doctype, d.docname))
            # get_unread_update_logs will have the current log
            old_logs = get_unread_update_logs(consumer.name, d.ref_doctype, d.docname)
            if old_logs:
                old_logs.reverse()
                result.extend(old_logs)
        else:
            result.append(d)

    for d in result:
        mark_consumer_read(update_log_name=d.name, consumer_name=consumer.name)

    result.reverse()
    return result
def is_consumer_uptodate(update_log, consumer):
	"""
	Checks if Consumer has read all the UpdateLogs before the specified update_log
	:param update_log: The UpdateLog Doc in context
	:param consumer: The EventConsumer doc
	"""
	if update_log.update_type == "Create":
		# consumer is obviously up to date
		return True

	prev_logs = frappe.get_all(
		"Event Update Log",
		filters={
			"ref_doctype": update_log.ref_doctype,
			"docname": update_log.docname,
			"creation": ["<", update_log.creation],
		},
		order_by="creation desc",
		limit_page_length=1,
	)

	if not len(prev_logs):
		return False

	prev_log_consumers = frappe.get_all(
		"Event Update Log Consumer",
		fields=["consumer"],
		filters={
			"parent": prev_logs[0].name,
			"parenttype": "Event Update Log",
			"consumer": consumer.name,
		},
	)

	return len(prev_log_consumers) > 0
def mark_consumer_read(update_log_name, consumer_name):
	"""
	This function appends the Consumer to the list of Consumers that has 'read' an Update Log
	"""
	update_log = frappe.get_doc("Event Update Log", update_log_name)
	if len([x for x in update_log.consumers if x.consumer == consumer_name]):
		return

	frappe.get_doc(
		frappe._dict(
			doctype="Event Update Log Consumer",
			consumer=consumer_name,
			parent=update_log_name,
			parenttype="Event Update Log",
			parentfield="consumers",
		)
	).insert(ignore_permissions=True)
def get_local_doc(update):
    """Get the local document if created with a different name"""
    try:
        if not update.use_same_name:
            return frappe.get_doc(update.ref_doctype, {"remote_docname": update.docname})
        return frappe.get_doc(update.ref_doctype, update.docname)
    except frappe.DoesNotExistError:
        return None


def log_event_sync(update, event_producer, sync_status, error=None):
	"""Log event update received with the sync_status as Synced or Failed"""
	doc = frappe.new_doc("Event Sync Log")
	doc.update_type = update.update_type
	doc.ref_doctype = update.ref_doctype
	doc.status = sync_status
	doc.event_producer = event_producer
	doc.producer_doc = update.docname
	doc.data = frappe.as_json(update.data)
	doc.use_same_name = update.use_same_name
	doc.mapping = update.mapping if update.mapping else None
	#if update.use_same_name:
	doc.docname = update.docname
	#else:
	#	doc.docname = frappe.db.get_value(update.ref_doctype, {"remote_docname": update.docname}, "name")
	if error:
		doc.error = error
	doc.insert()


def get_mapped_update(update, producer_site):
	"""get the new update document with mapped fields"""
	mapping = frappe.get_doc("Document Type Mapping", update.mapping)
	if update.update_type == "Create":
		doc = frappe._dict(json.loads(update.data))
		mapped_update = mapping.get_mapping(doc, producer_site, update.update_type)
		update.data = mapped_update.get("doc")
		update.dependencies = mapped_update.get("dependencies", None)
	elif update.update_type == "Update":
		mapped_update = mapping.get_mapped_update(update, producer_site)
		update.data = mapped_update.get("doc")
		update.dependencies = mapped_update.get("dependencies", None)

	update["ref_doctype"] = mapping.local_doctype
	return update




@frappe.whitelist()
def resync(update):
	"""Retry syncing update if failed"""
	update = frappe._dict(json.loads(update))
	producer_site = get_producer_site(update.event_producer)
	event_producer = frappe.get_doc("Event Producer", update.event_producer)
	if update.mapping:
		update = get_mapped_update(update, producer_site)
		update.data = json.loads(update.data)
	return sync(update, producer_site, event_producer, in_retry=True)
def get_unread_update_logs(consumer_name, dt, dn):
	"""
	Get old logs unread by the consumer on a particular document
	"""
	already_consumed = [
		x[0]
		for x in frappe.db.sql(
			"""
		SELECT
			update_log.name
		FROM `tabEvent Update Log` update_log
		JOIN `tabEvent Update Log Consumer` consumer ON consumer.parent = %(log_name)s
		WHERE
			consumer.consumer = %(consumer)s
			AND update_log.ref_doctype = %(dt)s
			AND update_log.docname = %(dn)s
	""",
			{
				"consumer": consumer_name,
				"dt": dt,
				"dn": dn,
				"log_name": "update_log.name"
				if frappe.conf.db_type == "mariadb"
				else "CAST(update_log.name AS VARCHAR)",
			},
			as_dict=0,
		)
	]

	logs = frappe.get_all(
		"Event Update Log",
		fields=["update_type", "ref_doctype", "docname", "data", "name",'creation'],
		filters={"ref_doctype": dt, "docname": dn, "name": ["not in", already_consumed]},
		order_by="creation",
	)

	return logs