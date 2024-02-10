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
            create(update)
			#set_insert(update, event_producer)
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
def create(doc):
	doc = {'update_type': 'Create', 'ref_doctype': 'Sales Invoice', 'docname': 'ss000000000000007', 'data': '{\n "__unsaved": 1,\n "additional_discount_percentage": 0.0,\n "address_display": "Nur bazar t\\u00f6weregi<br>Da\\u015foguz<br>\\nTurkmenistan<br>\\n",\n "advances": [],\n "against_income_account": "\\u041f\\u0440\\u043e\\u0434\\u0430\\u0436\\u0438 - C",\n "allocate_advances_automatically": 0,\n "amount_eligible_for_commission": 0.0,\n "apply_discount_on": "Grand Total",\n "base_change_amount": 0.0,\n "base_discount_amount": 0.0,\n "base_grand_total": 0.0,\n "base_in_words": "TMM Zero only.",\n "base_net_total": 0.0,\n "base_paid_amount": 0.0,\n "base_rounded_total": 0.0,\n "base_rounding_adjustment": 0.0,\n "base_total": 0.0,\n "base_total_taxes_and_charges": 0.0,\n "base_write_off_amount": 0.0,\n "change_amount": 0.0,\n "commission_rate": 0.0,\n "company": "5Dogan",\n "contact_display": "Abadan market",\n "contact_email": "",\n "contact_mobile": "8.65.03.33.88.",\n "contact_person": "Abadan markett-Abadan markett",\n "conversion_rate": 1.0,\n "creation": "2024-02-11 04:49:29.153926",\n "currency": "TMM",\n "customer": "Abadan market",\n "customer_address": "nur bazar-\\u0412\\u044b\\u0441\\u0442\\u0430\\u0432\\u043b\\u0435\\u043d\\u0438\\u0435 \\u0441\\u0447\\u0435\\u0442\\u043e\\u0432",\n "customer_group": "\\u041a\\u043e\\u043c\\u043c\\u0435\\u0440\\u0447\\u0435\\u0441\\u043a\\u0438\\u0439 \\u0441\\u0435\\u043a\\u0442\\u043e\\u0440",\n "customer_name": "Abadan market",\n "debit_to": "\\u0414\\u0435\\u0431\\u0435\\u0442\\u043e\\u0440\\u044b - C",\n "disable_rounded_total": 1,\n "discount_amount": 0.0,\n "docstatus": 0,\n "doctype": "Sales Invoice",\n "dont_create_loyalty_points": 0,\n "due_date": "2024-02-18",\n "grand_total": 0.0,\n "group_same_items": 0,\n "idx": 0,\n "ignore_default_payment_terms_template": 0,\n "ignore_pricing_rule": 0,\n "in_words": "TMM Zero only.",\n "is_cash_or_non_trade_discount": 0,\n "is_consolidated": 0,\n "is_debit_note": 0,\n "is_discounted": 0,\n "is_internal_customer": 0,\n "is_opening": "No",\n "is_pos": 0,\n "is_return": 0,\n "items": [\n  {\n   "__unsaved": 1,\n   "actual_batch_qty": 0.0,\n   "actual_qty": 22.11,\n   "allow_zero_valuation_rate": 0,\n   "amount": 0.0,\n   "barcode": "000001",\n   "base_amount": 0.0,\n   "base_net_amount": 0.0,\n   "base_net_rate": 0.0,\n   "base_price_list_rate": 0.0,\n   "base_rate": 0.0,\n   "base_rate_with_margin": 0.0,\n   "conversion_factor": 1.0,\n   "cost_center": "\\u041e\\u0441\\u043d\\u043e\\u0432\\u043d\\u044b\\u0435 - C",\n   "creation": "2024-02-11 00:06:49.448007",\n   "delivered_by_supplier": 0,\n   "delivered_qty": 0.0,\n   "description": "Koke pay taze ay pechine",\n   "discount_account": "\\u041c\\u0430\\u0440\\u043a\\u0435\\u0442\\u0438\\u043d\\u0433\\u043e\\u0432\\u044b\\u0435 \\u0440\\u0430\\u0441\\u0445\\u043e\\u0434\\u044b - C",\n   "discount_amount": 0.0,\n   "discount_percentage": 0.0,\n   "docstatus": 0,\n   "doctype": "Sales Invoice Item",\n   "enable_deferred_revenue": 0,\n   "expense_account": "\\u0421\\u0435\\u0431\\u0435\\u0441\\u0442\\u043e\\u0438\\u043c\\u043e\\u0441\\u0442\\u044c \\u043f\\u0440\\u043e\\u0434\\u0430\\u043d\\u043d\\u044b\\u0445 \\u043f\\u0440\\u043e\\u0434\\u0443\\u043a\\u0442\\u043e\\u0432 - C",\n   "grant_commission": 1,\n   "has_item_scanned": 0,\n   "idx": 1,\n   "image": "",\n   "income_account": "\\u041f\\u0440\\u043e\\u0434\\u0430\\u0436\\u0438 - C",\n   "incoming_rate": 38.0,\n   "is_fixed_asset": 0,\n   "is_free_item": 0,\n   "item_code": "000001",\n   "item_group": "\\u0420\\u0430\\u0437\\u0432\\u0435\\u0441\\u043d\\u044b\\u0435 \\u0442\\u043e\\u0432\\u0430\\u0440\\u044b",\n   "item_name": "Koke pay taze ay pechine",\n   "item_tax_rate": "{}",\n   "margin_rate_or_amount": 0.0,\n   "margin_type": "",\n   "modified": "2024-02-11 04:49:29.153926",\n   "modified_by": "Administrator",\n   "name": "84063e2457",\n   "net_amount": 0.0,\n   "net_rate": 0.0,\n   "owner": "Administrator",\n   "page_break": 0,\n   "parent": "ss000000000000007",\n   "parentfield": "items",\n   "parenttype": "Sales Invoice",\n   "posa_is_offer": 0,\n   "posa_offer_applied": 0,\n   "price_list_rate": 0.0,\n   "pricing_rules": "",\n   "qty": 1.0,\n   "rate": 0.0,\n   "rate_with_margin": 0.0,\n   "stock_qty": 1.0,\n   "stock_uom": "\\u043a\\u0433",\n   "stock_uom_rate": 0.0,\n   "total_weight": 0.0,\n   "uom": "\\u043a\\u0433",\n   "warehouse": "\\u041c\\u0430\\u0433\\u0430\\u0437\\u0438\\u043d\\u044b - C",\n   "weight_per_unit": 0.0\n  }\n ],\n "language": "ru",\n "letter_head": "Default",\n "loyalty_amount": 0.0,\n "loyalty_points": 0,\n "modified": "2024-02-11 04:49:29.153926",\n "modified_by": "Administrator",\n "name": "ss000000000000007",\n "naming_series": "ACC-SINV-.YYYY.-",\n "net_total": 0.0,\n "only_include_allocated_payments": 0,\n "outstanding_amount": 0.0,\n "owner": "Administrator",\n "packed_items": [],\n "paid_amount": 0.0,\n "party_account_currency": "TMM",\n "payment_schedule": [\n  {\n   "__unsaved": 1,\n   "base_payment_amount": 0.0,\n   "creation": "2024-02-11 00:06:49.448007",\n   "description": "7 Gundan son Polny tolag atmali",\n   "discount": 0.0,\n   "discount_date": "2024-02-11",\n   "discount_type": "Percentage",\n   "discounted_amount": 0.0,\n   "docstatus": 0,\n   "doctype": "Payment Schedule",\n   "due_date": "2024-02-18",\n   "idx": 1,\n   "invoice_portion": 100.0,\n   "modified": "2024-02-11 04:49:29.153926",\n   "modified_by": "Administrator",\n   "name": "c1c104f9c1",\n   "outstanding": 0.0,\n   "owner": "Administrator",\n   "paid_amount": 0.0,\n   "parent": "ss000000000000007",\n   "parentfield": "payment_schedule",\n   "parenttype": "Sales Invoice",\n   "payment_amount": 0.0,\n   "payment_term": "7Gun"\n  }\n ],\n "payment_terms_template": "7 Gun",\n "payments": [],\n "plc_conversion_rate": 1.0,\n "po_no": "",\n "posa_coupons": [],\n "posa_delivery_charges_rate": 0.0,\n "posa_is_printed": 0,\n "posa_offers": [],\n "posting_date": "2024-02-11",\n "posting_time": "04:49:29.438965",\n "price_list_currency": "TMM",\n "pricing_rules": [],\n "redeem_loyalty_points": 0,\n "remarks": "No Remarks",\n "repost_required": 0,\n "rounded_total": 0.0,\n "rounding_adjustment": 0.0,\n "sales_team": [],\n "selling_price_list": "Pra\\u00fds.Zakaz",\n "set_posting_time": 0,\n "status": "Draft",\n "tax_category": "",\n "tax_id": "",\n "taxes": [],\n "territory": "Turkmenistan",\n "timesheets": [],\n "title": "Abadan market",\n "total": 0.0,\n "total_advance": 0.0,\n "total_billing_amount": 0.0,\n "total_billing_hours": 0.0,\n "total_commission": 0.0,\n "total_net_weight": 0.0,\n "total_qty": 1.0,\n "total_taxes_and_charges": 0.0,\n "update_billed_amount_in_delivery_note": 1,\n "update_billed_amount_in_sales_order": 0,\n "update_stock": 0,\n "use_company_roundoff_cost_center": 0,\n "workflow_state": "\\u0417\\u0430\\u043a\\u0430\\u0437 \\u043f\\u043e\\u043b\\u0443\\u0447\\u0435\\u043d",\n "write_off_amount": 0.0,\n "write_off_outstanding_amount_automatically": 0\n}', 'name': 32, 'creation': '2024-02-11T04:49:29.600499', 'use_same_name': 1}
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