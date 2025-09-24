"""Streamlit app for Outbox action with improved structure and error handling."""

import json
import logging
import re
import time
from contextlib import suppress
from typing import Any, Callable, Dict, List, Optional, Union

import pandas as pd
import streamlit as st
import yaml
from jvclient.lib.utils import call_api, get_reports_payload
from jvclient.lib.widgets import app_controls, app_header, app_update_action
from requests import HTTPError
from streamlit_router import StreamlitRouter

# Constants
API_TIMEOUT = 30
AUTO_REFRESH_INTERVAL = 5
PAGE_SIZES = [5, 10, 20, 50, 100, 200]
DEFAULT_PAGE_SIZE = 10


# Validation and sanitization helpers
def validate_job_id(job_id: str) -> bool:
    """Validate job ID format (alphanumeric with dashes)."""
    return not job_id or bool(re.match(r"^[a-zA-Z0-9\-_]+$", job_id))


def validate_item_id(item_id: str) -> bool:
    """Validate item ID format (UUID)."""
    return not item_id or bool(
        re.match(
            r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", item_id
        )
    )


def sanitize_html(content: str) -> str:
    """TODO: Safe HTML sanitization."""
    return content


class StateManager:
    """Centralized session state management for Outbox action."""

    def __init__(self, agent_id: str, action_id: str) -> None:
        """
        Initialize Outbox action state manager.

        :param agent_id: Agent ID
        :param action_id: Action ID
        """
        self.prefix = f"outbox_{agent_id}_{action_id}"

    def get(
        self, key: str, default: Union[str, int, float, bool, list, dict, None] = None
    ) -> Union[str, int, float, bool, list, dict, None]:
        """
        Retrieve a value from the session state.

        :param key: Key to retrieve
        :param default: Default value if key is not found
        :return: Retrieved value or default if not found
        """
        return st.session_state.get(f"{self.prefix}_{key}", default)

    def set(
        self, key: str, value: Union[str, int, float, bool, list, dict, None]
    ) -> None:
        """
        Set a value in the session state.

        :param key: Key to set
        :param value: Value to set
        :return: None
        """
        st.session_state[f"{self.prefix}_{key}"] = value

    def delete(self, key: str) -> None:
        """
        Delete a value from the session state.

        :param key: Key to delete
        :return: None
        """
        if f"{self.prefix}_{key}" in st.session_state:
            del st.session_state[f"{self.prefix}_{key}"]

    def init_state(
        self, key: str, default: Union[str, int, float, bool, list, dict, None]
    ) -> None:
        """
        Initialize a session state variable to a default value if it does not exist.

        :param key: Key to initialize
        :param default: Default value to initialize to if key is not found
        :return: None
        """
        if f"{self.prefix}_{key}" not in st.session_state:
            self.set(key, default)

    def clear_all(self) -> None:
        """
        Clear all session state variables set by this Outbox action.

        :return: None
        """
        keys = list(st.session_state.keys())
        for key in keys:
            if key.startswith(self.prefix):
                del st.session_state[key]


# Set up logging
logger = logging.getLogger(__name__)


def handle_api_call(
    endpoint: str, json_data: Dict[str, Any], success_message: Optional[str] = None
) -> Union[Dict[str, Any], str, None]:
    """
    Standardized API call handler with error handling and logging.

    :param endpoint: API endpoint to call
    :param json_data: JSON payload for the request
    :param success_message: Message to display on success
    :return: Parsed response data or None on failure
    """
    try:
        result = call_api(endpoint=endpoint, json_data=json_data, timeout=API_TIMEOUT)
        if not result:
            raise ConnectionError("No response from API")
        if result.status_code != 200:
            raise ConnectionError(
                f"API returned status {result.status_code}: {result.text}"
            )

        response_data = get_reports_payload(result)
        if not response_data:
            return None

        if success_message:
            st.success(success_message)
        return response_data

    except ConnectionError as ce:
        error_msg = f"Connection error: {str(ce)}"
        logger.error(error_msg)
        st.error(error_msg)
    except HTTPError as he:
        error_msg = f"HTTP error {he.response.status_code}: {he.response.text}"
        logger.error(error_msg)
        st.error(f"API request failed with status {he.response.status_code}")
    except Exception as e:
        error_msg = f"Unexpected error: {str(e)}"
        logger.exception(error_msg)
        st.error(error_msg)

    return None


def validated_input(
    label: str,
    value: str,
    validation_fn: Callable[[str], bool],
    error_msg: str,
    key: str,
) -> Optional[str]:
    """
    Create a validated text input with error messaging.

    :param label: Input label
    :param value: Default value
    :param validation_fn: Validation function
    :param error_msg: Error message to display
    :param key: Unique key for Streamlit widget
    :return: Validated value or None if invalid
    """
    input_val = st.text_input(label, value, key=key)
    if input_val and not validation_fn(input_val):
        st.error(error_msg)
        return None
    return input_val


# Section rendering functions
def _render_export_outbox(state: StateManager, model_key: str, agent_id: str) -> None:
    """Render the Export Outbox section."""
    with st.expander("Export Outbox", False):
        if st.button(
            "Export Outbox",
            key=f"{model_key}_btn_export_outbox",
            disabled=not agent_id,
        ):
            outbox_result = handle_api_call(
                endpoint="action/walker/outbox_action/export_outbox",
                json_data={"agent_id": agent_id},
                success_message="Export outbox successfully",
            )

            if outbox_result:
                st.download_button(
                    label="Download Exported Outbox",
                    data=json.dumps(outbox_result, indent=2),
                    file_name="exported_outbox.json",
                    mime="application/json",
                )
                st.json(outbox_result)


def _render_import_outbox(state: StateManager, model_key: str, agent_id: str) -> None:
    """Render the Import Outbox section."""
    with st.expander("Import Outbox", False):
        outbox_source = st.radio(
            "Choose data source:",
            ("Text input", "Upload file"),
            key=f"{model_key}_outbox_source",
        )

        raw_text_input = ""
        uploaded_file = None
        data_to_import = None

        if outbox_source == "Text input":
            raw_text_input = st.text_area(
                "Outbox in YAML or JSON",
                value="",
                height=170,
                key=f"{model_key}_outbox_data",
            )

        if outbox_source == "Upload file":
            uploaded_file = st.file_uploader(
                "Upload file (YAML or JSON)",
                type=["yaml", "json"],
                key=f"{model_key}_agent_outbox_upload",
            )

        purge_collection = st.checkbox(
            "Purge Collection", value=False, key=f"{model_key}_purge_collection"
        )

        if st.button(
            "Import Outbox",
            key=f"{model_key}_btn_import_outbox",
            disabled=not agent_id,
        ):
            try:
                if outbox_source == "Upload file" and uploaded_file:
                    file_content = uploaded_file.read().decode("utf-8")
                    if uploaded_file.type == "application/json":
                        data_to_import = json.loads(file_content)
                    else:
                        data_to_import = yaml.safe_load(file_content)

                elif outbox_source == "Text input" and raw_text_input.strip():
                    try:
                        data_to_import = json.loads(raw_text_input)
                    except json.JSONDecodeError:
                        data_to_import = yaml.safe_load(raw_text_input)

                if data_to_import is None:
                    st.error("No valid outbox data provided.")
                else:
                    outbox = data_to_import.get("outbox", data_to_import)
                    handle_api_call(
                        endpoint="action/walker/outbox_action/import_outbox",
                        json_data={
                            "agent_id": agent_id,
                            "outbox": outbox,
                            "purge_collection": purge_collection,
                        },
                        success_message="Import outbox successfully",
                    )
            except Exception as e:
                logger.error(f"Import outbox failed: {str(e)}", exc_info=True)
                st.error(f"Import failed: {e}")


def _render_resend_failed_outbox(
    state: StateManager, model_key: str, agent_id: str
) -> None:
    """Render the Resend Failed Outbox section."""
    with st.expander("Resend Failed Outbox", False):
        if st.button("Resend", key=f"{model_key}_btn_resend_outbox_item"):
            response = handle_api_call(
                endpoint="action/walker/outbox_action/resend_failed_outbox",
                json_data={"agent_id": agent_id},
                success_message="Resend outbox successfully",
            )
            if response and isinstance(response, dict):
                st.success(response.get("message"))
                if response.get("sessions"):
                    st.write("Failed sessions:")
                    st.write(response.get("sessions"))


def _render_purge_outbox(state: StateManager, model_key: str, agent_id: str) -> None:
    """Render the Purge Outbox section."""
    with st.expander("Purge Outbox", False):
        job_id = validated_input(
            "OutboxJob ID to purge",
            "",
            validate_job_id,
            "OutboxJob ID must be alphanumeric with dashes and underscores only",
            f"{model_key}_job_id",
        )

        status = st.multiselect(
            "Status",
            ["PENDING", "PROCESSED", "FAILED"],
            key=f"{model_key}_status",
        )

        item_id = validated_input(
            "Item ID to purge",
            "",
            validate_item_id,
            "Item ID must be a valid UUID format",
            f"{model_key}_item_id",
        )

        purge_outbox = not (item_id or job_id or status)

        if job_id:
            button_text = "Yes, Purge outbox item"
            message = "Outbox item purged successfully"
            confirm_message = "Are you sure you want to purge the outbox item? This action cannot be undone."
        else:
            button_text = "Yes, Purge outbox"
            message = "Outbox purged successfully"
            confirm_message = "Are you sure you want to purge the outbox? This action cannot be undone."

        if st.button("Purge", key=f"{model_key}_btn_purge_outbox_item"):
            state.set("confirm_purge_collection", True)
            state.set("purge_outbox_item", None)

        if state.get("confirm_purge_collection", False):
            st.warning(confirm_message, icon="⚠️")
            col1, col2 = st.columns(2)

            with col1:
                if st.button(button_text):
                    purge_outbox_item: Union[str, int, float, list, dict, None] = (
                        handle_api_call(
                            endpoint="action/walker/outbox_action/purge_outbox",
                            json_data={
                                "purge": purge_outbox,
                                "agent_id": agent_id,
                                "job_id": job_id,
                                "status": status,
                                "item_id": item_id,
                            },
                            success_message=None,
                        )
                    )
                    state.set("confirm_purge_collection", False)
                    state.set("purge_outbox_item", purge_outbox_item)

            with col2:
                if st.button("No, cancel"):
                    state.set("confirm_purge_collection", False)
                    state.set("purge_outbox_item", None)
                    st.rerun()

        purge_outbox_item = state.get("purge_outbox_item")
        if purge_outbox_item:
            st.success(message)
            state.set("purge_outbox_item", None)
            time.sleep(2)
            st.rerun()
        elif purge_outbox_item in [False, []]:
            st.error(
                "Failed to purge outbox. Ensure that there is something to purge or check functionality"
            )
            state.set("purge_outbox_item", None)
            time.sleep(2)
            st.rerun()


@st.cache_data(ttl=300, show_spinner="Loading outbox...")
def _get_outbox_data(
    agent_id: str,
    page: int,
    per_page: int,
    session_filter: List[str],
    status_filter: List[str],
) -> Dict[str, Any]:
    """Fetch outbox data with caching."""
    response = handle_api_call(
        endpoint="action/walker/outbox_action/list_outbox_items",
        json_data={
            "agent_id": agent_id,
            "page": page,
            "limit": per_page,
            "filtered_sessions": session_filter,
            "filtered_status": status_filter,
        },
        success_message=None,
    )
    if response and isinstance(response, dict):
        return response

    return {}


def _render_outbox(state: StateManager, agent_id: str) -> None:
    """Render the Outbox section."""
    with st.expander("Outbox", True):
        args = {
            "agent_id": agent_id,
            "page": state.get("current_page", 1),
            "per_page": state.get("per_page", DEFAULT_PAGE_SIZE),
            "session_filter": state.get("session_id", []),
            "status_filter": state.get("status", []),
        }

        data = _get_outbox_data(**args)
        if not data:
            st.warning("No outbox items found")
            return

        total_items = data.get("total_items", 0)
        processed_items = data.get("processed", 0)
        pending_items = data.get("pending", 0)
        failed_items = data.get("failed", 0)
        total_pages = data.get("total_pages", 1)

        if not data.get("items"):
            st.warning("No outbox messages found")
            return

        df = prepare_data(data["items"])

        if df.empty:
            st.warning("No outbox messages found")
            return

        if "date" in df.columns and "time" in df.columns:
            df["datetime"] = df["date"].astype(str) + " " + df["time"].astype(str)
            with suppress(Exception):
                df["datetime"] = pd.to_datetime(df["datetime"])

        col1, col2, col3 = st.columns(3)
        with col1:
            prev_status_filter = state.get("status", [])
            status_filter = st.multiselect(
                "Filter by status",
                options=sorted(
                    [
                        "FAILED",
                        "PENDING",
                        "PROCESSED",
                        "PROCESSING",
                        "SENT",
                        "INCOMPLETE",
                        "COMPLETED",
                        "CANCELLED",
                    ]
                ),
                default=prev_status_filter,
            )
            if status_filter != prev_status_filter:
                state.set("status", status_filter)
                state.set("current_page", 1)  # Reset to first page when filter changes
                st.rerun()

        with col2:
            prev_batch_filter = state.get("session_id", [])
            batch_filter = st.multiselect(
                "Filter by Session ID",
                options=sorted(data["sessions"]),
                default=prev_batch_filter,
            )
            if batch_filter != prev_batch_filter:
                state.set("session_id", batch_filter)
                state.set("current_page", 1)  # Reset to first page when filter changes
                st.rerun()

        with col3:
            per_page_value = state.get("per_page", DEFAULT_PAGE_SIZE)
            if not isinstance(per_page_value, int):
                per_page_value = DEFAULT_PAGE_SIZE

            per_page = st.selectbox(
                "Items per page",
                options=PAGE_SIZES,
                index=PAGE_SIZES.index(per_page_value),
                key="per_page_selector",
            )
            if per_page != state.get("per_page"):
                state.set("per_page", per_page)
                state.set(
                    "current_page", 1
                )  # Reset to first page when page size changes
                st.rerun()

        st.dataframe(
            df[["id", "session_id", "content", "message_type", "status", "datetime"]],
            column_config={
                "id": "Message ID",
                "session_id": "Session ID",
                "content": st.column_config.TextColumn("Content", width="large"),
                "message_type": "Type",
                "status": st.column_config.TextColumn("Status", width="small"),
                "datetime": "Date & Time",
            },
            hide_index=True,
            use_container_width=True,
        )

        current_page = state.get("current_page", 1)
        if not isinstance(current_page, int):
            current_page = 1
        col1, col2, col3 = st.columns([2, 4, 2])
        with col1:
            if st.button("⬅️ Previous Page", disabled=current_page <= 1):
                state.set("current_page", current_page - 1)
                st.rerun()
        with col2:
            st.markdown(
                f"<div style='text-align: center;'>"
                f"Showing {len(df)} of {total_items} messages "
                f"(Page {current_page} of {total_pages})"
                f"</div>",
                unsafe_allow_html=True,
            )
        with col3:
            if st.button("Next Page ➡️", disabled=current_page >= total_pages):
                state.set("current_page", current_page + 1)
                st.rerun()

        st.write("---")
        st.subheader("Message Statistics")
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Total Messages", total_items)
        col2.metric("Processed Messages", processed_items)
        col3.metric("Pending Messages", pending_items)
        col4.metric("Failed Messages", failed_items)

        # Add refresh button for outbox
        if st.button("🔄 Refresh Outbox", key="refresh_outbox_btn"):
            st.cache_data.clear()
            st.rerun()


def prepare_data(data: list) -> pd.DataFrame:
    """
    Transforms message data into a pandas DataFrame.

    Args:
        data: List of message dictionaries

    Returns:
        DataFrame containing extracted message data
    """
    items = [
        {
            "id": msg["id"],
            "status": msg["status"],
            "session_id": msg["session_id"],
            "message_type": msg["message"]["message_type"],
            "content": str(msg["message"]["content"]),
            "created_on": msg["created_on"],
        }
        for msg in data
    ]

    df = pd.DataFrame(items)

    if not df.empty:
        df["created_on"] = pd.to_datetime(df["created_on"])
        df["date"] = df["created_on"].dt.date
        df["time"] = df["created_on"].dt.time

    return df


def render(router: StreamlitRouter, agent_id: str, action_id: str, info: dict) -> None:
    """Main render function for WhatsApp Connect action."""
    state = StateManager(agent_id, action_id)
    model_key, module_root = app_header(agent_id, action_id, info)

    # Initialize state variables individually
    state.init_state("session_payload", {})
    state.init_state("confirm_purge_collection", False)
    state.init_state("purge_outbox_item", None)
    state.init_state("current_page", 1)
    state.init_state("per_page", DEFAULT_PAGE_SIZE)
    state.init_state("session_id", [])
    state.init_state("status", [])
    state.init_state("last_refresh", 0)

    with st.expander("Outbox Configuration", expanded=False):
        app_controls(agent_id, action_id)
        app_update_action(agent_id, action_id)

    # Render sections
    _render_export_outbox(state, model_key, agent_id)
    _render_import_outbox(state, model_key, agent_id)
    _render_resend_failed_outbox(state, model_key, agent_id)
    _render_purge_outbox(state, model_key, agent_id)
    _render_outbox(state, agent_id)
