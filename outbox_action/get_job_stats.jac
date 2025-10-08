import logging;
import from logging { Logger }
import from jivas.agent.core.agent { Agent }
import from jivas.agent.action.action { Action }
import from jivas.agent.action.actions { Actions }
import from jivas.agent.modules.action.path { action_walker_path }
import from jivas.agent.action.agent_graph_walker { agent_graph_walker }


walker get_job_stats(agent_graph_walker) {

    has page:int = 1;
    has limit:int = 10;
    has job_id:str = '';
    has response:dict = {};
    has reporting:bool = True;


    # set up logger
    static has logger:Logger = logging.getLogger(__name__);

    class __specs__ {
        static has private: bool = False;
        static has path: str = action_walker_path(__module__);
    }

    can on_agent with Agent entry {
        visit [-->](`?Actions);
    }

    can on_actions with Actions entry {
        visit [-->](`?Action)(?enabled==True)(?label=='OutboxAction');
    }

    can on_action with Action entry {
        self.response = here.get_job_stats(page=self.page, limit=0, job_id=self.job_id);  # fetch all documents

        if self.reporting {
            report self.response;
        }
    }

}