
import log_collector


def fail_to_connect():
    raise RuntimeError("Simulated Kafka startup failure")


# Replace publisher initialization with a controlled failure.
log_collector.LogKafkaPublisher = fail_to_connect

# Run main() and verify that it handles the startup error.
log_collector.main()

print("\nPASS: Collector handled the startup failure.")
