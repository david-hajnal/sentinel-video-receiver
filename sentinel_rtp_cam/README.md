# Create motion detect event

## ONVIF Setip

```
python3 -m pip install --upgrade onvif-python
onvif --discover --interactive
```

```
# Enter these commands:

devicemgmt

events

admin@192.168.1.187:2020/events > CreatePullPointSubscription
{
    'SubscriptionReference': {
        'Address': {
            '_value_1': 'http://192.168.1.187:1024/event-1024_1024',
            '_attr_1': None
        },
        'ReferenceParameters': None,
        'Metadata': None,
        '_value_1': None,
        '_attr_1': None
    },
    'CurrentTime': datetime.datetime(2026, 1, 12, 16, 52, 16, tzinfo=<isodate.tzinfo.Utc object at 0x108d81190>),
    'TerminationTime': datetime.datetime(2026, 1, 12, 17, 2, 16, tzinfo=<isodate.tzinfo.Utc object at 0x108d81190>),
    '_value_1': None
}
```