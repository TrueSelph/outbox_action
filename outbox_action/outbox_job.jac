import logging;
import from logging { Logger }
import from datetime { datetime, timezone }
import from jivas.agent.core.graph_node { GraphNode }
import from actions.jivas.outbox_action.outbox_item { OutboxItem }
import from actions.jivas.outbox_action.outbox_status { OutboxStatus }
import from jivas.agent.memory.interaction_response { InteractionMessage }
import from jivas.agent.modules.data.commit { commit }


node OutboxJob(GraphNode) {
    has collection_id:str = "";
    has status:OutboxStatus = OutboxStatus.PROCESSING;
    has callback_url:str = "";
    has sender_action:str = ""; # e.g. the action which implements outbox send functionality
    has created_on:str = str((datetime.now(timezone.utc)).isoformat());
    has completed_on:str = "";
    has total:int = 0;
    has success_rate:float = 0.0;

    # set up logger
    static has logger:Logger = logging.getLogger(__name__);

    def get_status() -> OutboxStatus {
        return self.status;
    }

    def set_status(status:OutboxStatus) -> None {
        # timestamp completed on
        if status in [OutboxStatus.COMPLETED, OutboxStatus.FAILED, OutboxStatus.CANCELLED, OutboxStatus.PROCESSING, OutboxStatus.INCOMPLETE, OutboxStatus.SENT, OutboxStatus.PENDING] {
            self.completed_on = str((datetime.now(timezone.utc)).isoformat());
        }
        self.status = status;
    }

    def get_job_id() -> str {
        return self.id;
    }

    def get_sender_action() -> str {
        return self.sender_action;
    }

    def get_success_rate() -> float {
        return self.success_rate;
    }

    def get_total_items() -> int {
        return self.total;
    }

    def get_callback_url() -> str {
        return self.callback_url;
    }

    def set_success_rate(success_rate:float) {
        self.success_rate = success_rate;
    }

    def get_items() -> list {
        # retrieves a list of outbox items
        return [-->](`?OutboxItem);
    }

    def get_item(id:str) -> OutboxItem {
        # retrieves an attached document entry by id
        return node_obj([-->](`?OutboxItem)(?id == id));
    }

    def add_item(
        message:InteractionMessage,
        session_id:str="",
        channel:str="default",
        status:OutboxStatus=OutboxStatus.PENDING
    ) -> OutboxItem {
        # adds an outbox item to this job entry
        if not all([
            isinstance(session_id, str),
            isinstance(message, InteractionMessage)
        ]) {
            self.logger.error("Invalid parameters for outbox item");
            return;
        }
        item = OutboxItem(
            collection_id = self.collection_id,
            job_id = self.id,
            sender_action = self.sender_action,
            message = message,
            session_id = session_id,
            channel = channel,
            status=status
        );

        self ++> item;
        self.total += 1;

        commit(item);

        return item;
    }

    def delete_item(id:str) -> bool {
        # removes an attached outbox item by id
        item = node_obj([-->](`?OutboxItem)(?id == id));
        if not item {
            return False;
        }
        Jac.destroy( item );
        self.total -= 1;
        # if this is the last entry in this job, also remove the job itself
        if not [-->](`?OutboxItem) {
            self.delete();
        }
        return True;
    }

    def delete() -> list {
        return (self spawn _purge_job()).removed;
    }

    walker _purge_job {
        # walker which carries out the traversal and purging of jobs and any related docs

        has removed:list = [];

        obj __specs__ {
            # make this a private walker
            static has private: bool = True;
        }

        can on_job with OutboxJob entry {
            visit [-->];
            self.removed.append(here);
            Jac.destroy(here);
        }

        can on_job_item with OutboxItem entry {
            self.removed.append(here);
            Jac.destroy(here);
        }
    }
}
