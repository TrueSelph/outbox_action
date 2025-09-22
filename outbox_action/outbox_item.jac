import from datetime { datetime, timezone }
import from jivas.agent.core.graph_node { GraphNode }
import from actions.jivas.wppconnect_action.outbox_status { OutboxStatus }
import from jivas.agent.modules.interaction.message { InteractionMessage }
import from jivas.agent.modules.system.common { node_obj }

node OutboxItem(GraphNode) {
    has collection_id:str = "";
    has job_id:str = "";
    has status:OutboxStatus = OutboxStatus.PENDING;
    has created_on:str = str((datetime.now(timezone.utc)).isoformat());
    has message:InteractionMessage = None;
    has session_id:str = "";
    has channel:str = "default"; # e.g. whatsapp, email, sms, etc

    def get_job() {
        return node_obj([<--]);
    }

    def get_job_id() {
        return self.job_id;
    }

    def get_status() -> OutboxStatus {
        return self.status;
    }

    def set_status(status:OutboxStatus) -> None {
        # timestamp completed on
        if status in [OutboxStatus.SENT, OutboxStatus.FAILED, OutboxStatus.CANCELLED] {
            self.completed_on = str((datetime.now(timezone.utc)).isoformat());
        }
        self.status = status;
    }

    def get_message() -> InteractionMessage {
        return self.message;
    }

    def set_message(message:InteractionMessage) -> None {
        self.message = message;
    }

    def get_session_id() -> str {
        return self.session_id;
    }

    
}