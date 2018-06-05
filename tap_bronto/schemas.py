from funcy import project

from datetime import datetime
from singer import metadata

def with_properties(properties, key_properties, valid_replication_keys):
    return_schema = {
        'type': 'object',
        'properties': properties,
        'additionalProperties': False
    }

    mdata = metadata.new()

    for field, schema in return_schema.get('properties').items():
        if field in key_properties:
            mdata = metadata.write(mdata, ('properties', field), 'inclusion', 'automatic')
        else:
            mdata = metadata.write(mdata, ('properties', field), 'inclusion', 'available')

    # TODO: Add more here?
    mdata = metadata.write(mdata, (), 'table-key-properties', key_properties)
    mdata = metadata.write(mdata, (), 'valid-replication-keys', valid_replication_keys)

    return return_schema, metadata.to_list(mdata)


def is_selected(catalog, field=None):
    mdata = metadata.to_map(catalog.get('metadata'))

    if not field:
        return mdata.get((), {}).get('selected')
    else:
        # TODO: Fix logic for selected
        field_metadata = mdata.get(('properties', field), {})
        return field_metadata.get('selected', False) or field_metadata.get('inclusion', False) == 'automatic'

def get_field_selector(catalog, schema):
    selections = []

    for field, schema in schema.get('properties').items():
        if is_selected(catalog, field):
            selections.append(field)

    def select(data):
        to_return = {}

        for k, v in project(data, selections).items():
            if isinstance(v, datetime):
                to_return[k] = v.replace(microsecond=0).isoformat()

            else:
                to_return[k] = v

        return to_return

    return select


ACTIVITY_SCHEMA = {
    'id': {
        'type': ['string'],
        'description': ('Manufactured unique ID for the activity.')
    },
    'createdDate': {
        'type': ['string'],
        'description': ('The date the activity was recorded.')
    },
    'contactId': {
        'type': ['null', 'string'],
        'description': ('The ID assigned to the contact '
                        'associated with the activity.')
    },
    'activityType': {
        'type': ['string'],
        'description': ('The type of activity the object '
                        'represents. For outbound activities, '
                        'this can be `send` or `sms_send`.')
    },
    'listId': {
        'type': ['null', 'string'],
        'description': ('The ID assigned to the list that '
                        'the delivery associated with the '
                        'activity was sent to.')
    },
    'segmentId': {
        'type': ['null', 'string'],
        'description': ('The ID assigned to the segment that '
                        'the delivery associated with the activity '
                        'was sent to.')
    },
    'keywordId': {
        'type': ['null', 'string'],
        'description': ('The ID assigned to the SMS keyword that '
                        'the SMS delivery associated with the '
                        'activity was sent to.')
    },
    'messageId': {
        'type': ['null', 'string'],
        'description': ('The ID assigned to the message associated '
                        'with the activity.')
    },
    'deliveryId': {
        'type': ['null', 'string'],
        'description': ('The ID assigned to the delivery '
                        'associated with the activity.')
    },
    'workflowId': {
        'type': ['null', 'string'],
        'description': ('The ID assigned to the workflow that '
                        'sent the delivery associated with the '
                        'activity.')
    },
    'emailAddress': {
        'type': ['null', 'string'],
        'description': ('The email address of the contact '
                        'associated with the activity. The '
                        'emailAddress property is returned if '
                        'a contactId is returned, and an email '
                        'address is stored for the associated '
                        'contact.')
    },
    'mobileNumber': {
        'type': ['null', 'string'],
        'description': ('The mobile number of the contact '
                        'associated with the activity. The '
                        'mobileNumber property is returned if '
                        'a contactId is returned, and a mobile '
                        'number is stored for the associated '
                        'contact.')
    },
    'contactStatus': {
        'type': ['null', 'string'],
        'description': ('The status of the contact associated '
                        'with the activity. Status can be '
                        '`active`, `onboarding`, `transactional`, '
                        '`bounce`, `unconfirmed`, or `unsub`')
    },
    'messageName': {
        'type': ['null', 'string'],
        'description': ('The name of the message associated with '
                        'the activity. The messageName property '
                        'is returned if a messageId is returned.')
    },
    'deliveryType': {
        'type': ['null', 'string'],
        'description': ('The type of delivery associated with the '
                        'activity: `bulk`, `test`, `split`, '
                        '`trigger`, or `ftaf` (forward to a '
                        ' friend).')
    },
    'deliveryStart': {
        'type': ['null', 'string'],
        'description': ('The date/time the delivery associated '
                        'with the activity was scheduled. The '
                        'deliveryStart property is returned if '
                        'a deliveryId is returned.')
    },
    'workflowName': {
        'type': ['null', 'string'],
        'description': ('The name of the workflow associated '
                        'with the activity. The workflowName '
                        'property is returned if a workflowId '
                        'is returned.')
    },
    'segmentName': {
        'type': ['null', 'string'],
        'description': ('The name of the segment associated with '
                        'the activity. The segmentName property '
                        'is returned if a segmentId is returned.')
    },
    'listName': {
        'type': ['null', 'string'],
        'description': ('The name of the list associated with '
                        'the activity. The listName property is '
                        'returned if a listId is returned.')
    },
    'listLabel': {
        'type': ['null', 'string'],
        'description': ('The label assigned to the list associated '
                        'with the activity. The label is the '
                        'external (customer facing) name given to '
                        'a list. The listLabel property is returned '
                        'if a listId is returned.')
    },
    'automatorName': {
        'type': ['null', 'string'],
        'description': ('The name of the automator associated with '
                        'the activity.')
    },
    'smsKeywordName': {
        'type': ['null', 'string'],
        'description': ('The name of the SMS keyword associated '
                        'with the activity. The smsKeywordName '
                        'property is returned if a keywordId is '
                        'returned.')
    },
    'bounceType': {
        'type': ['null', 'string'],
        'description': (
            'The type of bounce recorded. The following types can '
            'be returned: Hard Bounces: bad_email, destination_'
            'unreachable, rejected_message_content. Soft Bounces: '
            'temporary_contact_issue, destination_temporarily_'
            'unavailable, deferred_message_content, unclassified. '
            'The bounceType property is returned if the '
            'activityType is bounce.')
    },
    'bounceReason': {
        'type': ['null', 'string'],
        'description': ('The detailed reason why the bounce '
                        'occurred. The bounceReason property is '
                        'returned if the activityType is bounce.')
    },
    'skipReason': {
        'type': ['null', 'string'],
        'description': ('The detailed reason why the contact '
                        'was skipped when attempting to send to '
                        'them. The skipReason property is returned '
                        'if the activityType is contactskip.')
    },
    'linkName': {
        'type': ['null', 'string'],
        'description': ('The name of the link that was clicked. '
                        'The linkName property is returned if the '
                        'activityType is click.')
    },
    'linkUrl': {
        'type': ['null', 'string'],
        'description': ('The URL of the link that was clicked. '
                        'The linkUrl property is returned if '
                        'the activityType is click.')
    },
    'orderId': {
        'type': ['null', 'string'],
        'description': ('The ID assigned to the order. The '
                        'orderId property is returned if the '
                        'activityType is conversion.')
    },
    'unsubscribeMethod': {
        'type': ['null', 'string'],
        'description': ('The method used by the contact to '
                        'unsubscribe. Valid values are: subscriber'
                        'admin, bulk, listcleaning, fbl (Feedback '
                        'Loop), complaint, account, api, '
                        'unclassified. The unsubscribeMethod '
                        'property is returned if the activityType '
                        'is unsubscribe.')
    },
    'ftafEmails': {
        'type': ['null', 'string'],
        'description': ('The emails that were used in the Forward '
                        'To A Friend Delivery. The ftafEmails '
                        'property is returned if the activityType '
                        'is friendforward.')
    },
    'socialNetwork': {
        'type': ['null', 'string'],
        'description': ('The social network the activity was '
                        'performed on. The valid networks are: '
                        'facebook, twitter, linkedin, digg, '
                        'myspace. The bounceType property is '
                        'returned if the activityType is social.')
    },
    'socialActivity': {
        'type': ['null', 'string'],
        'description': ('The activity performed. The valid '
                        'activities are: view, share. The '
                        'socialActivity property is returned '
                        'if the activityType is social.')
    },
    'webformId': {
        'type': ['null', 'string'],
        'description': ('The unique ID for a webform.')
    },
    'webformAction': {
        'type': ['null', 'string'],
        'description': ('The activity performed on the webform. '
                        'Valid values are: submitted, view. The '
                        'webformAction property is returned if '
                        'the activityType is webform.')
    },
    'webformName': {
        'type': ['null', 'string'],
        'description': ('The name of the webform used. The '
                        'webformName property is returned if the '
                        'activityType is webform.')
    }
}

CONTACT_SCHEMA = {
    'id': {
        'type': ['string'],
        'description': ('The unique id for the contact. The id can '
                        'be used to reference a specific contact '
                        'when using the contact functions.')
    },
    'email': {
        'type': ['null', 'string'],
        'description': ('The email address assigned to the '
                        'contact. The email address can be used to '
                        'reference a specific contact when using '
                        'the contact functions.')
    },
    'mobileNumber': {
        'type': ['null', 'string'],
        'description': ('The mobile number stored for the contact. '
                        'A valid country code must be included when '
                        'adding or updating a mobile number for a '
                        'contact.')
    },
    'status': {
        'type': ['null', 'string'],
        'description': ('The status of the contact. Valid statuses '
                        'are: active, onboarding, transactional, '
                        'bounce, unconfirmed, unsub')
    },
    'msgPref': {
        'type': ['null', 'string'],
        'description': ('The message preference for the contact. '
                        'A contact can have a message preference '
                        'of text or html.')
    },
    'source': {
        'type': ['null', 'string'],
        'description': ('The source or where the contact came '
                        'from. The source can manual, import, api, '
                        'webform, or sforcereport (salesforce report).')
    },
    'customSource': {
        'type': ['null', 'string'],
        'description': ('A source you define that states where '
                        'the contact came from.')
    },
    'created': {
        'type': ['null', 'string'],
        'description': ('The date the contact was created. This '
                        'timestamp is immutable and cannot be '
                        'changed')
    },
    'modified': {
        'type': ['null', 'string'],
        'description': ('The last time information about the '
                        'contact was modified. This timestamp is '
                        'immutable and cannot be changed.')
    },
    'deleted': {
        'type': ['null', 'boolean'],
        'description': 'Set to true if the contact has been deleted.'
    },
    'listIds': {
        'type': ['null', 'array'],
        'items': {
            'type': 'string'
        },
        'description': ('The lists (referenced by ID) that the '
                        'contact belongs to. You can obtain listIds '
                        'by calling the readLists function.')
    },
    'SMSKeywordIDs': {
        'type': ['null', 'array'],
        'items': {
            'type': 'string'
        },
        'description': ('An array of the ids corresponding to '
                        'SMS keywords the contact is subscribed to.')
    },
    'numSends': {
        'type': ['null', 'number'],
        'description': ('The total number of deliveries sent to '
                        'the contact.')
    },
    'numBounces': {
        'type': ['null', 'number'],
        'description': ('The total number of times deliveries '
                        'sent to the contact resulted in a bounce.')
    },
    'numOpens': {
        'type': ['null', 'number'],
        'description': ('The total number of times deliveries were '
                        'opened by the contact. This metric includes '
                        'multiple opens of the same delivery.')
    },
    'numClicks': {
        'type': ['null', 'number'],
        'description': ('The total number of times deliveries were '
                        'clicked by the contact. If a link is clicked '
                        'multiple times, each click is included in '
                        'this metric.')
    },
    'numConversions': {
        'type': ['null', 'number'],
        'description': ('The total number of conversions made by '
                        'the contact.')
    },
    'conversionAmount': {
        'type': ['null', 'number'],
        'description': ('The sum/total amount of conversions made '
                        'by the contact.')
    },
    'geoIPCity': {
        'type': ['null', 'string'],
        'description': ('The city recorded for the contact '
                        'based on their last known non-mobile '
                        'IP addresses.')
    },
    'geoIPStateRegion': {
        'type': ['null', 'string'],
        'description': ('The state/region recorded for the '
                        'contact based on their last known '
                        'non-mobile IP addresses.')
    },
    'geoIPZip': {
        'type': ['null', 'string'],
        'description': ('The zip code recorded for the contact '
                        'based on their last known non-mobile '
                        'IP addresses.')
    },
    'geoIPCountry': {
        'type': ['null', 'string'],
        'description': ('The country recorded for the contact '
                        'based on their last known non-mobile '
                        'IP addresses.')
    },
    'geoIPCountryCode': {
        'type': ['null', 'string'],
        'description': ('The country code recorded for the '
                        'contact based on their last known '
                        'non-mobile IP addresses.')
    },
    'primaryBrowser': {
        'type': ['null', 'string'],
        'description': ('The primary browser (Firefox, Chrome, '
                        'Safari, etc.) used by a contact.')
    },
    'mobileBrowser': {
        'type': ['null', 'string'],
        'description': ('The mobile browser (Safari mobile, '
                        'Firefox mobile, Chrome mobile) used '
                        'by a contact.')
    },
    'primaryEmailClient': {
        'type': ['null', 'string'],
        'description': ('The primary email client (Microsoft '
                        'Outlook, Mozilla Thunderbird, Apple '
                        'Mail, etc.) used by a contact.')
    },
    'mobileEmailClient': {
        'type': ['null', 'string'],
        'description': ('The mobile email client (Gmail mobile, '
                        'Yahoo Mail for mobile, etc.) used by '
                        'a contact.')
    },
    'operatingSystem': {
        'type': ['null', 'string'],
        'description': ('The operating system (MacOSX, WinXP, '
                        'Win7, Android, iOS etc.) used by a contact.')
    },
    'firstOrderDate': {
        'type': ['null', 'string'],
        'description': ('The date of the first order recorded for '
                        'a contact.')
    },
    'lastOrderDate': {
        'type': ['null', 'string'],
        'description': ('The date of the last order recorded for '
                        'a contact.')
    },
    'lastOrderTotal': {
        'type': ['null', 'number'],
        'description': ('The total amount of revenue recorded for '
                        'the most recent order.')
    },
    'totalOrders': {
        'type': ['null', 'number'],
        'description': ('The total number of orders recorded for '
                        'a contact.')
    },
    'totalRevenue': {
        'type': ['null', 'number'],
        'description': ('The total amount of revenue recorded '
                        'for a contact.')
    },
    'averageOrderValue': {
        'type': ['null', 'number'],
        'description': ('The average amount of revenue per order '
                        'recorded for a contact.')
    },
    'lastDeliveryDate': {
        'type': ['null', 'string'],
        'description': ('The last date a delivery was made '
                        'to the contact.')
    },
    'lastOpenDate': {
        'type': ['null', 'string'],
        'description': ('The last date an open was recorded '
                        'for the contact.')
    },
    'lastClickDate': {
        'type': ['null', 'string'],
        'description': ('The last date a click was recorded '
                        'for the contact.')
    }
}
