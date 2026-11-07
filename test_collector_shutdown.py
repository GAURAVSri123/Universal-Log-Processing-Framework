
import log_collector


class FakePublisher:
    def __init__(self):
        self.closed = False

    def close(self):
        self.closed = True
        print("[TEST] Publisher close() called.")


fake_publisher = FakePublisher()


def create_fake_publisher():
    return fake_publisher


# Avoid connecting to Kafka or processing real log files.
log_collector.LogKafkaPublisher = create_fake_publisher

# Make the collection loop fail immediately after startup.
def fail_discovery():
    raise RuntimeError("Simulated collection-loop failure")


log_collector.discover_log_files = fail_discovery

log_collector.main()

assert fake_publisher.closed, "Publisher was not closed."

print("PASS: Publisher cleanup was attempted.")
print("PASS: Shutdown test completed.")
