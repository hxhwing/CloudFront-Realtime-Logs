from __future__ import print_function
import boto3
import base64
import json

client = boto3.client('sns')
# Include your SNS topic ARN here.
#topic_arn = 'arn:aws:sns:<region>:<account_id>:<topic_name>'
waf = boto3.client('waf')
get_token = waf.get_change_token()
token = get_token['ChangeToken']

ipsetid = '08c77039-3669-40eb-9fe6-989d16f1fc0a'


def lambda_handler(event, context):
    output = []
    success = 0
    failure = 0
    for record in event['records']:
        try:
            # Uncomment the below line to publish the decoded data to the SNS topic.
            payload = base64.b64decode(record['data'])
            print(payload)
            data = json.loads(payload)
            if data["CLIENTIP_COUNT"] > 100:
                ip = data['CLIENTIP'] + '/32'
                print(ip)
                response = waf.update_ip_set(
                    ChangeToken = token,
                    IPSetId= ipsetid,
                    Updates=[
                        {
                            'Action': 'INSERT',
                            'IPSetDescriptor': {
                                'Type': 'IPV4',
                                'Value': ip,
                            },
                        },
                    ],
                )
                print(response)
            #client.publish(TopicArn=topic_arn, Message=payload, Subject='Sent from Kinesis Analytics')
            output.append({'recordId': record['recordId'], 'result': 'Ok'})
            success += 1
        except Exception:
            output.append({'recordId': record['recordId'], 'result': 'DeliveryFailed'})
            failure += 1

    print('Successfully delivered {0} records, failed to deliver {1} records'.format(success, failure))
    return {'records': output}
