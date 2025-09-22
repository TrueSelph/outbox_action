import from enum { unique }

@unique
enum OutboxStatus {
    PENDING = 'PENDING', # job/item is being created
    PROCESSING = 'PROCESSING', # job/item is currently being processed
    INCOMPLETE = 'INCOMPLETE', # job completed but some items failed
    COMPLETED = 'COMPLETED', # job completed successfully
    SENT = 'SENT', # set when item is sent
    CANCELLED = 'CANCELLED', # job is cancelled
    FAILED = 'FAILED', # job/item failed to complete
}