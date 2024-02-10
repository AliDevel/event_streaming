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
    frappe.log_error(frappe.get_traceback(), 'payment failed')

    if isinstance(update, str):
        update = frappe.parse_json(update)
        frappe.log_error(frappe.get_traceback(), frappe.parse_json(update))

    event_producer = event_producer# event_producer
    frappe.log_error(frappe.get_traceback(), 'Hi')

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

	frappe.log_error(frappe.get_traceback(), 'insert1')	
	frappe.log_error(frappe.get_traceback(), str(update.data))
	doc = frappe.get_doc(update.data)
	frappe.log_error(frappe.get_traceback(), 'insert2')	
	
	#if update.use_same_name:
	doc.insert(set_name=update.docname, set_child_names=False)
	
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