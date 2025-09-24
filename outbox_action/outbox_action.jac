import os;
import uuid;
import shutil;
import logging;
import traceback;
import mimetypes;
import base64;
import random;
import from datetime { datetime, timezone }
import dateutil.parser;
import from typing { Union }
import from logging { Logger }
import from jivas.agent.memory.collection { Collection }
import from jivas.agent.action.action { Action }
import from jivas.agent.memory.interaction_response {
    MessageType, InteractionMessage, TextInteractionMessage, MediaInteractionMessage, MultiInteractionMessage
}

import from jac_cloud.plugin.jaseci { JacPlugin as Jac }
import from jivas.agent.core.graph_node { GraphNode }
import from jac_cloud.core.archetype {BaseCollection, NodeAnchor, WalkerAnchor}
import from jivas.agent.modules.data.node_pager { NodePager }
import from jivas.agent.modules.system.common { node_obj }
import from jivas.agent.modules.data.node_get { node_get }
import from jivas.agent.modules.data.commit { commit }
import from actions.jivas.outbox_action.outbox_status { OutboxStatus }
import from actions.jivas.outbox_action.outbox_job { OutboxJob }
import from actions.jivas.outbox_action.outbox_item { OutboxItem }


node OutboxAction(Action) {
    # Handles the queueing and scheduled mass dispatch of messages (WhatsApp, email, SMS, etc) to recipients

    # set up logger
    static has logger:Logger = logging.getLogger(__name__);

    has base_url:str = ""; # Jivas Base URL
    # API settings
    has request_timeout:float = 10.0; # the length of time this action waits for api to complete request

    # outbox settings
    has outbox_base_rate_per_minute:int = 20;  # WhatsApp API rate limit for sending messages
    has outbox_send_interval:float = 1.0; # the interval (delay) in seconds between sending messages from the outbox
    has outbox_min_send_interval:float = 1.0; # the minimum interval (delay) in seconds between sending messages from the outbox
    has outbox_max_send_interval:float = 10.0; # the maximum interval (delay) in seconds between sending messages from the outbox
    has outbox_min_batch_size:int = 1; # the minimum batch size of messages to send from the outbox
    has outbox_max_batch_size:int = 10; # the maximum batch size of messages to send from the outbox
    # poll settings
    has last_purge_outbox:str = ""; # the last time the outbox was purged
    has outbox_threshold_days:int = 7; # the number of days to keep messages in the outbox

    def postinit {
        super.postinit();
        # list of node attributes which should be excluded from export
        self.transient_attrs += ['last_purge_outbox'];
    }

    def on_enable() {
        self.start_outbox_scheduler();
    }

    def on_disable() {
        self.stop_outbox_scheduler();
    }

    def post_register() {
        # post register procedure to start the outbox scheduler
        self.start_outbox_scheduler();
    }

    def pulse() {
        # Main scheduling pulse with adaptive rate control
        if not self.has_outbox_items() {
            self.stop_outbox_scheduler();
            return;
        }

        # Calculate dynamic batch size based on current rate window
        batch_size = self.calculate_dynamic_batch_size();

        # Process messages with current batch size
        (processed, successes, failures) = self.batch_send(batch_size=batch_size);

        # Update rate limiting parameters
        self.update_send_parameters(successes, failures);

        # Restart scheduler with new parameters if items remain
        if self.has_outbox_items() {
            self.restart_scheduler();
        }
        last_purge_outbox = datetime.now().strftime("%Y-%m-%d");

        if self.last_purge_outbox != last_purge_outbox {
            self.last_purge_outbox = last_purge_outbox;
            self.purge_outbox(threshold_days=self.outbox_threshold_days);
        }
    }

    def logout_session() {
        self.api().logout_session();
        return True;
    }

    # --------------- OUTBOX SCHEDULER ----------------

    def restart_scheduler() {
        # Gracefully update scheduler with new parameters
        self.stop_outbox_scheduler();
        self.start_outbox_scheduler();
        self.logger.debug(f"Outbox scheduler restarted with interval {self.outbox_send_interval}s");
    }

    def has_outbox_items() -> bool {
        # check if there are any pending outbox items
        pending_items = self.get_outbox_items(status=OutboxStatus.PENDING);
        self.logger.info(f"Pending outbox items: {len(pending_items)}");
        if len(pending_items) > 0 {
            return True;
        }
        return False;
    }

    def start_outbox_scheduler() {
        self.logger.info(f"Starting outbox scheduler");

        # Proceed with scheduler start
        if self.has_outbox_items() {
            self.logger.info(f"Starting outbox scheduler 2");
            if pulse_action := self.get_agent().get_action(action_label="PulseAction") {
                self.logger.info(f"Starting outbox scheduler 3");
                pulse_action.add_schedule(
                    self.get_type(),
                    f'every({self.outbox_send_interval}).seconds'
                );
                self.logger.info(
                    f"Outbox scheduler started: {self.outbox_send_interval}s interval "
                    f"(Batch size: {self.outbox_max_batch_size})"
                );
            }
        }
    }

    def stop_outbox_scheduler() {
        # stop the outbox scheduler

        if(pulse_action := self.get_agent().get_action(action_label="PulseAction")) {
            # remove the outbox scheduler from the pulse action
            pulse_action.remove_schedule(self.get_type());
            self.logger.info(f"Outbox scheduler stopped");
        }
    }

    def send_outbox_items(outbox_items:list) -> (int, int, int) {
        # sends a list of outbox items and updates the job status accordingly
        self.logger.info(f"Sending {len(outbox_items)} outbox items");

        processed_count = 0;
        success_count = 0;
        failure_count = 0;

        for item in outbox_items {

            # try {

            # grab the job for the item
            job = item.get_job();
            sender_action_label = job.get_sender_action();
            session_id = item.get_session_id();
            message = item.get_message();
            channel = item.get_channel();

            # perform checks
            if not sender_action_label or not session_id or not message {
                raise Exception(f"Outbox item {item_id} missing required fields");
            }

            # load the reference to the sender action
            sender_action = self.get_agent().get_action(action_label=sender_action_label);

            if not sender_action {
                raise Exception(f"Sender action {sender_action_label} not found for outbox item {item_id}");
            }

            if not hasattr(sender_action, 'send_outbox_message') {
                raise Exception(f"Sender action {sender_action} does not implement send_outbox_message method");
            }

            # attempt to send the message by calling the sender action's send_outbox_message method
            sent = sender_action.send_outbox_message(
                session_id=session_id,
                message=message
            );

            if sent {
                # update the status of the item
                item.set_status(OutboxStatus.SENT);
                success_count += 1;
                new_success_rate = (job.get_success_rate() + success_count) / job.get_total_items();
                job.set_success_rate(new_success_rate);

                self.update_interaction_context(session_id, message, channel);

            } else {

                item.set_status(OutboxStatus.FAILED);
                failure_count += 1;

            }

            processed_count += 1;

            # } except Exception as e {

            #     self.logger.error(f"Outbox message processing failed for [{item.export()}]: {str(e)}");
            #     item.set_status(OutboxStatus.FAILED);
            #     failure_count += 1;
            #     processed_count += 1;

            # }
        }

        return (processed_count, success_count, failure_count);
    }

    def batch_send(batch_size: int = 1) -> (int, int, int) {
        self.logger.info(f"Sending batch of {batch_size} outbox items");
        # send items from outbox with batch processing
        if not self.has_outbox_items() {
            return (0, 0, 0);
        }

        processed_count = 0;
        success_count = 0;
        failure_count = 0;
        pending_items = self.get_outbox_items(status=OutboxStatus.PENDING);
        self.logger.info(f"Pending outbox items: {len(pending_items)}");
        self.logger.info(f"processed_count: {processed_count}");
        self.logger.info(f"success_count: {success_count}");
        self.logger.info(f"failure_count: {failure_count}");

        while processed_count < batch_size and len(pending_items) > 0 {

            # Process batch slice
            processable = min(batch_size - processed_count, len(pending_items));
            items = list(pending_items)[:processable];


            # Dispatch the items and update job state
            if items and len(items) > 0 {
                (processed, success, failure) = self.send_outbox_items(items);
                processed_count += processed;
                success_count += success;
                failure_count += failure;
            }
            item = pending_items.pop(0);

            # Update job state
            self.maybe_finalize_job(item.job_id);
        }

        return (processed_count, success_count, failure_count);
    }

    def batch_resend(batch_size: int = 1) -> dict {
        # Resend all failed items from outbox with batch processing

        found_failed_outbox = False;
        jobs = self.get_jobs();
        if jobs and len(jobs) > 0 {
            for (job_id, job) in jobs.items() {
                if len(job['failed']) > 0 {
                    found_failed_outbox = True;
                }
            }
        }

        if not found_failed_outbox {
            return {"message": f"No failed outbox items found."};
        }

        processed_count = 0;
        success_count = 0;
        failure_count = 0;

        failed_sessions = [];
        jobs = list(self.get_jobs().items());

        while processed_count < batch_size and len(jobs) > 0 {
            (job_id, job) = jobs.pop(0);

            # Process batch slice
            processable = min(batch_size - processed_count, len(job['failed']));
            items = list(job['failed'].items())[:processable];

            # Dispatch the items and update job state
            if items and len(items) > 0 {
                (processed, success, failure) = self.send_outbox_items(items);
                processed_count += processed;
                success_count += success;
                failure_count += failure;
            }

            # Update job state
            self.maybe_finalize_job(job_id, job);
        }

        return {"message": f"Processed: {processed_count}, Success: {success_count}, Failure: {failure_count}","sessions":failed_sessions};
    }

    def add_outbox_job(messages: list, sender_action:str, callback_url: str = "", status:OutboxStatus = OutboxStatus.PENDING, job_id: str = "") -> str {
        # Adds a batch of messages to the outbox with comprehensive validation
        # Returns job ID if successful, empty string if no valid messages

        if not messages or not isinstance(messages, list) {
            self.logger.error("Invalid messages format - expected list of message dicts");
            return "";
        }

        # self.logger.info(messages);

        valid_items = 0;
        validation_errors = [];

        # create a new outbox job
        job = None;
        collection = self.get_collection();
        self.logger.info(f"Adding outbox job with {len(messages)} messages. collection id: {collection.id}");

        try{
            # if job_id is not provided, create a new job
            if job_id and &job_id {
                job = &job_id;
            }
        } except Exception as e {
            self.logger.warning(f"Failed to create outbox job with id {job_id}: {str(e)}");
        }

         if not job {
            job = OutboxJob(
                collection_id = collection.id,
                sender_action = sender_action,
                callback_url = callback_url,
                status=OutboxStatus.PROCESSING
            );
            # map the job node
            collection ++> job;

            commit(job);

        }

        for (index, item) in enumerate(messages) {
            self.logger.info(f"type: {type(item)}");
            # self.logger.info(f"item: {item}");
            validation_errors = self.validate_message_item(item, index, validation_errors);
            if validation_errors {
                self.logger.error(f"Message {index} validation failed");
                self.logger.error(f"{validation_errors}");
                continue;
            }

            try {
                interaction_message = self.prepare_interaction_message(
                    message=item['message']
                );
                job.add_item(
                    session_id=item['to'],
                    message=interaction_message,
                    channel=item.get('channel', "default"),
                    status=status
                );
                valid_items += 1;
            } except Exception as e {
                error_msg = f"Message {index} processing failed: {str(e)}";
                validation_errors.append(error_msg);
                self.logger.error(error_msg);
            }
        }

        # Clean up empty jobs
        if valid_items == 0 {
            self.purge_outbox(job_id=job.id);
            self.logger.warning(f"OutboxJob {job.id} contained no valid messages");
            if validation_errors {
                self.logger.debug(f"Validation errors:\n" + "\n".join(validation_errors));
            }
            return "";
        }

        # Start scheduler only if valid messages exist
        self.start_outbox_scheduler();
        return job_id;
    }

    def get_outbox_jobs() -> list {
        # retrieves a list of outbox jobs
        collection = self.get_collection();

        jobs = node_get({
            "name": "OutboxJob",
            "archetype.collection_id": collection.id,
        });

        return jobs;
    }

    def get_outbox_items(status:OutboxStatus = None, job_id:str = "") -> list {
        # retrieves a list of outbox items, optionally filtered by status and or job_id
        collection = self.get_collection();

        query = {
            "name": "OutboxItem",
            "archetype.collection_id": collection.id
        };
        # witems = node_get({"name": "OutboxItem"});
        # self.logger.info(f"collection.id: {collection.id}");
        # self.logger.info(f"witems: {witems}");

        if status {
            query["archetype.status"] = str(status.value);
        }

        if job_id {
            query["archetype.job_id"] = job_id;
        }
        self.logger.info(f"get_outbox_items query: {query}");

        items = node_get(query);
        # self.logger.info(f"items: {items}");

        return list(items);
    }

    def update_interaction_context(session_id:str, message:InteractionMessage, channel:str="default") {
        # Shared context update logic
        agent_node = self.get_agent();
        frame_node = agent_node.get_memory().get_frame(
            agent_id=agent_node.id,
            session_id=session_id,
            force_session=True
        );
        frame_node.add_unprompted_interaction(
            message=message.get_content(),
            channel=channel
        );
    }

    def prepare_interaction_message(message:dict) -> InteractionMessage {
        # prepare the interaction message object based on the dict representation
        if not message {
            return None;
        }

        message_type = message.get('message_type');

        if message_type == MessageType.TEXT.value {
            interaction_message = TextInteractionMessage();
            interaction_message.load(message);
        } elif message_type == MessageType.MEDIA.value {
            interaction_message = MediaInteractionMessage();
            interaction_message.load(message);
        } elif message_type == MessageType.MULTI.value {
            interaction_message = MultiInteractionMessage();
            for message_item in message.get("content") {
                message_item_type = message_item.get("message_type");
                if message_item_type == MessageType.TEXT.value {
                    interaction_message.add_interaction_message(TextInteractionMessage(content=message_item.get("content")));
                } elif message_item_type == MessageType.MEDIA.value {
                    interaction_message.add_interaction_message(MediaInteractionMessage(mime=message_item.get("mime"), content=message_item.get("content"), data=message_item.get("data")));
                }
            }
        } else {
            return None;
        }

        return interaction_message;
    }

    def validate_message_item(item: dict, index: int, error_log: list) -> bool {
        # Validates individual message items with detailed error reporting
        required_keys = {'to', 'message'};
        message_types = {'TEXT', 'MEDIA', 'MULTI'};

        if not isinstance(item, dict) {
            error_log.append(f"Item {index}: Not a dictionary");
            return error_log;
        }

        missing_keys = required_keys - item.keys();
        if missing_keys {
            error_log.append(f"Item {index}: Missing keys {missing_keys}");
            return error_log;
        }

        message = item.get('message', {});
        msg_type = message.get('message_type', '').upper();

        if msg_type not in message_types {
            error_log.append(f"Item {index}: Invalid message type '{msg_type}'");
            return error_log;
        }

        # Type-specific validation
        validation_map = {
            'TEXT': self.validate_text_message,
            'MEDIA': self.validate_media_message,
            'MULTI': self.validate_multi_message
        };

        if not validation_map[msg_type](message, index) {
            return error_log;
        }

        return error_log;
    }

    def validate_text_message(message: dict, index: int) -> bool {
        if not message.get('content') {
            self.logger.error(f"Item {index}: TEXT message missing content");
            return False;
        }
        return True;
    }

    def validate_media_message(message: dict, index: int) -> bool {
        required = {'mime', 'content', 'data'};
        file_type = self.get_file_type(mime_type=message.get('mime', 'unknown'));

        if(file_type.get('file_type') == "poll") {
            required.remove('content');
        }

        missing = required - message.keys();

        if missing {
            self.logger.error(f"Item {index}: MEDIA message missing {missing}");
            return False;
        }

        if (file_type.get('file_type') == "poll" and all([key in message.get('data', {}) for key in ['name', 'choices', 'options']])) {
            return True;
        }

        if 'url' not in message['data'] {
            self.logger.error(f"Item {index}: MEDIA message missing URL");
            return False;
        }
        return True;
    }

    def validate_multi_message(message: dict, index: int) -> bool {
        components = message.get('content', []);

        if not isinstance(components, list) or len(components) < 2 {
            self.logger.error(f"Item {index}: MULTI message requires 2+ components");
            return False;
        }

        for (i, comp) in enumerate(components) {
            if not comp.get('message_type') {
                self.logger.error(f"Item {index}: Component {i} missing message_type");
                return False;
            }
        }
        return True;
    }

    def create_job_structure(callback_url: str = "") -> dict {
        # Returns standardized job structure with metadata
        return {
            'pending': {},
            'processed': {},
            'failed': {},
            'callback_url': callback_url,
            'created_on': datetime.now().isoformat(),
            'stats': {
                'total': 0,
                'success_rate': 0.0
            }
        };
    }

    def maybe_finalize_job(job_id:str) {
        # updates job status and maybe triggers callback if job has no more pending items

        if not (job := &job_id) {
            self.logger.error(f"Job {job_id} not found");
            return;
        }

        failed_items = self.get_outbox_items(status=OutboxStatus.FAILED, job_id=job_id);
        sent_items = self.get_outbox_items(status=OutboxStatus.SENT, job_id=job_id);
        pending_items = self.get_outbox_items(status=OutboxStatus.PENDING, job_id=job_id);

        if len(pending_items) > 0 {
            job.set_status(OutboxStatus.PROCESSING);
        } elif len(pending_items) == 0 and len(failed_items) > 0 {
            job.set_status(OutboxStatus.INCOMPLETE);
        } elif len(pending_items) == 0 and len(failed_items) == 0 {
            job.set_status(OutboxStatus.COMPLETED);
        }

        if (callback := job.get_callback_url()) and (len(pending_items) == 0) {

            self.api().send_rest_request(
                endpoint=callback,
                method='POST',
                data={
                    'status': job.get_status(),
                    'job_id': job_id,
                    'success_count': len(sent_items),
                    'failure_count': len(failed_items),
                    'pending_count': len(pending_items),
                    'timestamp': datetime.now().isoformat()
                },
                json_body=True,
                use_full_url=True
            );
        }

    }

    def clamp(value:Union[int,float], min_val:Union[int,float], max_val:Union[int,float]) -> Union[int,float] {
        # Utility function for boundary constraints
        return max(min_val, min(value, max_val));
    }

    def calculate_dynamic_batch_size() -> int {
        # Calculate batch size based on success rate and current interval
        base_size = max(1, int(self.outbox_base_rate_per_minute * (self.outbox_send_interval / 60)));

        # Add jitter to prevent patterns
        jittered_size = base_size * random.uniform(0.8, 1.2);

        # Apply min/max constraints
        return self.clamp(int(jittered_size), self.outbox_min_batch_size, self.outbox_max_batch_size);
    }

    def update_send_parameters(successes: int, failures: int) {
        # Adaptive rate control algorithm
        total = successes + failures;
        success_rate = successes / total if total > 0 else 1.0;

        # Calculate interval adjustment
        if success_rate < 0.7 {
            # Degraded performance - slow down
            adjustment_factor = 1 + (0.7 - success_rate);
            self.outbox_send_interval *= adjustment_factor;
        } else {
            # Good performance - speed up cautiously
            adjustment_factor = 1 - (success_rate - 0.7) * 0.3;
            self.outbox_send_interval *= adjustment_factor;
        }

        # Apply jitter and constraints
        self.outbox_send_interval *= float(random.uniform(0.9, 1.1));
        self.outbox_send_interval = self.clamp(
            self.outbox_send_interval,
            self.outbox_min_send_interval,
            self.outbox_max_send_interval
        );
    }

    # # collection
    # def add_outbox_item_collection(status:OutboxStatus, job_id: str, item_id: str, session_id: str, message: InteractionMessage) -> OutboxJob {
    #     collection = self.get_collection();
    #     outbox_item = (collection spawn _add_job_collection(status=status, job_id=job_id, item_id=item_id, session_id=session_id, message=message, collection_id=collection.id)).outbox_item;
    #     if outbox_item {
    #         self.increment_job_count();
    #     }
    #     return outbox_item;
    # }

    def purge_outbox(purge:bool = False, threshold_days:int = 0, status:list = [], item_id:str = "", job_id:str = "") -> bool {
        collection = self.get_collection();
        result = (collection spawn _purge_outbox(purge=purge, threshold_days=threshold_days, status=status, item_id=item_id, job_id=job_id));

        deleted = result.deleted;
        deleted_job = result.deleted_job;
        deleted.extend(deleted_job);

        total_jobs = collection.data_get('job_count') or 0;
        amount = total_jobs - len(deleted_job);
        collection.data_set('job_count', amount);
        return len(deleted) > 0;
    }

    def import_outbox(outbox: dict, purge_collection: bool = False) -> bool {
        # try {
        collection = self.get_collection();

        # purge jobs collection
        if(purge_collection){
            self.purge_outbox(purge=True);
        }

        # import outbox
        for (job_id, job_data) in outbox.items() {

            job_status = job_data.get("job_info", {}).get("status", OutboxStatus.PROCESSING);
            job_callback_url = job_data.get("job_info", {}).get("callback_url", "");
            job_sender_action = job_data.get("job_info", {}).get("sender_action", "WPPConnectAction");
            if job_data.get("pending") {
                messages = [];
                for (item_id, outbox_item) in job_data.get("pending", {}).items() {
                    messages.append({
                        "to": outbox_item.get("session_id"),
                        "message": outbox_item.get("message", {}).get("context"),
                        "channel": outbox_item.get("channel", "whatsapp")
                    });
                }
                self.add_outbox_job(messages = messages, job_id = job_id, sender_action = job_sender_action, callback_url = job_callback_url, status=OutboxStatus.PENDING);
            }
            elif job_data.get("processed") {
                messages = [];
                for (item_id, outbox_item) in job_data.get("processed", {}).items() {
                    messages.append({
                        "to": outbox_item.get("session_id"),
                        "message": outbox_item.get("message", {}).get("context"),
                        "channel": outbox_item.get("channel", "whatsapp")
                    });
                }
                self.add_outbox_job(messages = messages, job_id = job_id, sender_action = job_sender_action, callback_url = job_callback_url, status=OutboxStatus.SENT);
            }
            elif job_data.get("failed") {
                messages = [];
                for (item_id, outbox_item) in job_data.get("failed", {}).items() {
                    messages.append({
                        "to": outbox_item.get("session_id"),
                        "message": outbox_item.get("message", {}).get("context"),
                        "channel": outbox_item.get("channel", "whatsapp")
                    });
                }
                self.add_outbox_job(messages = messages, job_id = job_id, sender_action = job_sender_action, callback_url = job_callback_url, status=OutboxStatus.FAILED);
            }
            # # for (item_id, job) in job_data['pending'].items() {
            # #     self.add_outbox_item_collection(status=OutboxStatus.PENDING, job_id=job_id, item_id=item_id, session_id=job['session_id'], message=job['message']);
            # # }
            # for (item_id, job) in job_data['processed'].items() {
            #     self.add_outbox_item_collection(status=OutboxStatus.PROCESSED, job_id=job_id, item_id=item_id, session_id=job['session_id'], message=job['message']);
            # }
            # for (item_id, job) in job_data['failed'].items() {
            #     self.add_outbox_item_collection(status=OutboxStatus.FAILED, job_id=job_id, item_id=item_id, session_id=job['session_id'], message=job['message']);
            # }
        }

        # # Calculate dynamic batch size based on current rate window
        # batch_size = self.calculate_dynamic_batch_size();

        # # Process messages with current batch size
        # (processed, successes, failures) = self.batch_send(batch_size=batch_size);

        # # Update rate limiting parameters
        # self.update_send_parameters(successes, failures);

        # # Restart scheduler with new parameters if items remain
        # if self.has_outbox_items() {
        #     self.restart_scheduler();
        # }
        self.start_outbox_scheduler();

        return True;
        # } except Exception as e {
        #     self.logger.error(f"Error importing outbox: {e}");
        #     return False;
        # }
    }


    def list_outbox_items(page:int, limit:int, filtered_status:list = [], filtered_sessions:list = []) -> list[dict] {
        # Lists all document entries in the manifest with pagination.

        collection = self.get_collection();
        # Initialize pager
        pager = NodePager(NodeAnchor.Collection, page_size=limit, current_page=page);

        inbox_item_filter = [{"name": "OutboxItem"}, {"archetype.collection_id": collection.id}];
        if (filtered_status and filtered_sessions) {
            inbox_item_filter = [{"name": "OutboxItem"}, {"archetype.collection_id": collection.id, "archetype.session_id": {"$in": filtered_sessions}, "archetype.status": {"$in": filtered_status}}];
        }elif(filtered_sessions) {
            inbox_item_filter = [{"name": "OutboxItem"}, {"archetype.collection_id": collection.id, "archetype.session_id": {"$in": filtered_sessions}}];
        }elif(filtered_status) {
            inbox_item_filter = [{"name": "OutboxItem"}, {"archetype.collection_id": collection.id, "archetype.status": {"$in": filtered_status}}];
        }

        items = pager.get_page({
            "$or": [
                {"$and": inbox_item_filter}
            ]
        });

        processed_items = NodeAnchor.Collection.count({
            "$and": [{"name": "OutboxItem"}, {"archetype.collection_id": collection.id}, {"archetype.status": {"$in": ["SENT"]}}]
        });

        failed_items = NodeAnchor.Collection.count({
            "$and": [{"name": "OutboxItem"}, {"archetype.collection_id": collection.id}, {"archetype.status": {"$in": ["FAILED"]}}]
        });

        pending_items = NodeAnchor.Collection.count({
            "$and": [{"name": "OutboxItem"}, {"archetype.collection_id": collection.id}, {"archetype.status": {"$in": ["PENDING"]}}]
        });

        if not items {
            return {};
        }

        outbox_items = [];
        sessions = [];
        for item in items {
            if item.session_id not in sessions {
                sessions.append(item.session_id);
            }
            outbox_items.append(item.export());
        }


        # call export on each and convert it to a dict
        items = [item.export() for item in items];
        # get all info as a dict
        pagination_info = pager.to_dict();

        return {
            "page": page,
            "limit": limit,
            "total_items": processed_items + pending_items + failed_items,
            "total_pages": pagination_info.get("total_pages", 1),
            "has_previous": pagination_info.get("has_previous", False),
            "has_next": pagination_info.get("has_next", False),
            "items": outbox_items,
            "sessions": sessions,
            "processed": processed_items,
            "pending": pending_items,
            "failed": failed_items
        };
    }

    def get_jobs(job_id:str = "") -> dict {
        collection = self.get_collection();
        return (collection spawn _get_jobs(job_id=job_id)).outbox_item;
    }

    def export_collection() -> dict {
        result = self.get_jobs();
        return {"outbox": result};
    }

    def import_collection(data:dict, purge:bool=True) -> bool {

        if data {
            outbox = data.get("outbox");
            self.import_outbox(outbox=outbox, purge_collection = purge);
            return True;
        }
        return False;
    }

    def get_file_type(file_path: Optional[str] = None, url: Optional[str] = None, mime_type: Optional[str] = None) -> dict {

        detected_mime_type = None;

        if file_path {
            # Use mimetypes to guess MIME type based on file extension
            detected_mime_type = mimetypes.guess_type(file_path)[0];
        } elif(url) {
            # Make a HEAD request to get the Content-Type header
            try {
                response = requests.head(url, allow_redirects=True);
                detected_mime_type = response.headers.get("Content-Type");
            } except requests.RequestException as e {
                self.logger.warning(f"Error making HEAD request: {e}");
            }
        } else {
            # Fallback to initial MIME type if provided
            detected_mime_type = mime_type;
        }

        # MIME type categories
        mime_categories = {
            "image": [
                "image/jpeg",
                "image/png",
                "image/gif",
                "image/bmp",
                "image/webp",
                "image/tiff",
                "image/svg+xml",
                "image/x-icon",
                "image/heic",
                "image/heif",
                "image/x-raw",
            ],
            "document": [
                "application/pdf",
                "application/msword",
                "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                "application/vnd.ms-excel",
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                "application/vnd.ms-powerpoint",
                "application/vnd.openxmlformats-officedocument.presentationml.presentation",
                "text/plain",
                "text/csv",
                "text/html",
                "application/rtf",
                "application/x-tex",
                "application/vnd.oasis.opendocument.text",
                "application/vnd.oasis.opendocument.spreadsheet",
                "application/epub+zip",
                "application/x-mobipocket-ebook",
                "application/x-fictionbook+xml",
                "application/x-abiword",
                "application/vnd.apple.pages",
                "application/vnd.google-apps.document",
            ],
            "audio": [
                "audio/mpeg",
                "audio/wav",
                "audio/ogg",
                "audio/flac",
                "audio/aac",
                "audio/mp3",
                "audio/webm",
                "audio/amr",
                "audio/midi",
                "audio/x-m4a",
                "audio/x-realaudio",
                "audio/x-aiff",
                "audio/x-wav",
                "audio/x-matroska",
            ],
            "video": [
                "video/mp4",
                "video/mpeg",
                "video/ogg",
                "video/webm",
                "video/quicktime",
                "video/x-msvideo",
                "video/x-matroska",
                "video/x-flv",
                "video/x-ms-wmv",
                "video/3gpp",
                "video/3gpp2",
                "video/h264",
                "video/h265",
                "video/x-f4v",
                "video/avi",
            ],
            "poll": [
                "application/poll",  # Generic and clean
                "application/vnd.jivas.poll",  # Vendor-specific to your framework
                "poll/message",  # Custom subtype under a new "poll" type
                "application/x-poll-data",  # Legacy-style custom type
                "application/jivas-poll+json",  # Jivas framework + structured data format
                "jivas/poll",  # Jivas framework + poll
            ]
        };

        # Handle cases where MIME type cannot be detected
        if not detected_mime_type or detected_mime_type == "binary/octet-stream" {
            file_extension = "";
            if file_path {
                file_extension = os.path.splitext(file_path)[1];
            } elif url {
                file_extension = os.path.splitext(url)[1];
            }

            detected_mime_type = mimetypes.types_map.get(
                file_extension.lower(), "unknown/unknown"
            );
        }

        # Categorize MIME type
        for (category, mime_list) in mime_categories.items() {
            if detected_mime_type in mime_list {
                return {"file_type": category, "mime": detected_mime_type};
            }
        }

        # Default to "unknown" if no category matches
        return {"file_type": "unknown", "mime": detected_mime_type};

    }

}

walker _get_jobs {
    has job_id:str = "";
    has outbox_item:dict = {};

    obj __specs__ {
        static has private:bool = True;
    }


    can on_collection with Collection entry {
        if self.job_id {
            visit [-->](`?OutboxJob)(?id == self.job_id);
        } else {
            visit [-->](`?OutboxJob);
        }
    }

    can on_job with OutboxJob entry {
        print(f"job_id: {here.id}");

        # get pending post
        pending_outbox_item_node = [-->](`?OutboxItem)(?status == OutboxStatus.PENDING);
        print(f"pending_outbox_item_node: {pending_outbox_item_node}");
        pending_outbox_item_data = {};
        for item in pending_outbox_item_node {
            pending_outbox_item_data[item.id] = {
                "session_id": item.session_id,
                "message": item.message,
                "created_on": item.created_on,
                "sender_action": item.sender_action,
                "channel": item.channel,
                "status": item.status
            };
        }

        # get processed outbox item
        processed_outbox_item_node = [-->](`?OutboxItem)(?status == OutboxStatus.SENT);
        processed_outbox_item_data = {};
        for item in processed_outbox_item_node {
            processed_outbox_item_data[item.id] = {
                "session_id": item.session_id,
                "message": item.message,
                "created_on": item.created_on,
                "sender_action": item.sender_action,
                "channel": item.channel,
                "status": item.status
            };
        }

        # get failed outbox item
        failed_outbox_item_node = [-->](`?OutboxItem)(?status == OutboxStatus.FAILED);
        failed_outbox_item_data = {};
        for item in failed_outbox_item_node {
            failed_outbox_item_data[item.id] = {
                "session_id": item.session_id,
                "message": item.message,
                "created_on": item.created_on,
                "sender_action": item.sender_action,
                "channel": item.channel,
                "status": item.status
            };
        }

        self.outbox_item[here.id] = {
            "pending": pending_outbox_item_data,
            "processed": processed_outbox_item_data,
            "failed": failed_outbox_item_data,
            "job_info": {
                "status": here.status,
                "callback_url": here.callback_url,
                "sender_action": here.sender_action,
                "created_on": here.created_on,
                "completed_on": here.completed_on,
                "total": here.total,
                "success_rate": here.success_rate
            },
            "success_rate": here.success_rate,
            "total": here.total
        };
    }
}


walker _purge_outbox {
    has purge:bool = False;
    has threshold_days:int = 0;
    has status:list = [];
    has item_id:str = "";
    has job_id:str = "";
    has deleted:list = [];
    has deleted_job:list = [];

    obj __specs__ {
        static has private:bool = True;
    }

    can on_collection with Collection entry {
        if self.purge {
            self.deleted.append(here);
        }

        visit [-->](`?OutboxJob)(?id == self.job_id) else {
            visit [-->](`?OutboxJob);
        }
    }

    can on_outbox_job with OutboxJob entry {
        if self.purge {
            self.deleted_job.append(here);
        } elif self.threshold_days > 0 {
            created_dt = datetime.fromisoformat(here.created_on);
            # now = datetime.now();
            # days_old = (now - created_dt).days;

            now = datetime.now(timezone.utc);
            created_dt = created_dt.astimezone(timezone.utc);
            days_old = (now - created_dt).days;

            if days_old >= self.threshold_days {
                self.deleted_job.append(here);
                self.job_id = here.id;
            }
        } elif here.id == self.job_id {
            self.deleted_job.append(here);
        } else {
            children_edges = [-->](`?OutboxItem);
            if(len(children_edges) <= 1) {
                self.deleted_job.append(here);
            }
        }

        visit [-->](`?OutboxItem)(?id == self.item_id) else {
            visit [-->](`?OutboxItem);
        }

    }

    can on_outbox_item with OutboxItem entry {
        if self.purge {
            self.deleted.append(here);
        } elif self.job_id {
            self.deleted.append(here);
        } elif self.item_id {
            if here.id == self.item_id {
                self.deleted.append(here);
            }
        } elif self.status {
            if here.status.value in self.status {
                self.deleted.append(here);
            }
        } else {
            self.deleted.append(here);
        }
    }

    can on_exit with exit {
        for item in self.deleted {
            Jac.destroy(item);
        }

        for item in self.deleted_job {
            Jac.destroy(item);
        }
    }
}