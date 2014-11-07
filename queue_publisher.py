from ceilometer import publisher
import pika

class QueuePublisher(publisher.PublisherBase):
    """Republishes all received samples to a rabbit queue"""

    def __init__(self, parsed_url):
        super(QueuePublisher).__init__(self, parsed_url)
        settings = {}
        f = open('ceilometer_queue_publisher.cfg', 'r')
        for line in f:
            [k, v] = line.split()
            settings[k] = v
        credentials = pika.credentials.PlainCredentials(username = settings['username'],
                                                        password = settings['password'],
                                                        erase_on_connect = True)
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=settings['host'],
                                                                            port=5672,
                                                                            virtual_host='/',
                                                                            credentials=credentials))
        self.channel = self.connection.channel()
        self.exchange = settings['exchange']

    def publish_samples(self, context, samples):
        """
        Converts each sample into a string of JSON and publishes it the setup rabbit queue
        """
        for sample in samples:
            message = str(sample.as_dict())
            self.channel.basic_publish(exchange=self.exchange,
                                       body=message)
