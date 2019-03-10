from google.cloud import pubsub_v1

project_id = 'red-dog-piano'
pubsub_topic = 'amount'


def publish_messages():
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, pubsub_topic)

    messages = ['{"id": 1,"value": 4}',
                '{"id": 1,"value": 84}',
                '{"id": 2,"value": 22}',
                '{"id": 3,"value": 763}',
                '{"id": 1,"value": 342}',
                '{"id": 5,"value": 102}',
                '{"id": 4,"value": 98}',
                '{"id": 4,"value": 5}',
                '{"id": 4,"value": 18}',
                '{"id": 4,"value": 78}',
                '{"id": 4,"value": 43}',
                '{"id": 1,"value": 76}']

    for message in messages:
        data = message.encode('utf-8')
        publisher.publish(topic_path, data=data)


if __name__ == '__main__':
    publish_messages()
