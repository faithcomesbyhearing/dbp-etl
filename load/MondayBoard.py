import json
import logging
from typing import List, Optional, Tuple, Any

from MondayHTTP import HTTP

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class MondayBoardBatch:
    """
    Equivalent to the Go 'MondayBoardBatch' struct.
    Used for batched updates/creates on a Monday board.
    """
    def __init__(self, board_monday_id: int, item_name: str, column_values: str):
        self.board_monday_id = board_monday_id
        self.item_name = item_name
        self.column_values = column_values


class BoardResponse:
    """
    Equivalent to the Go 'BoardResponse' struct, which wraps:
      data {
        boards []
      }
    """
    def __init__(self, response_dict: dict):
        self.response_dict = response_dict

    def validate(self) -> Tuple[bool, str]:
        """
        In Go:
          func (s BoardResponse) Validate() (bool, string)
        Checks if boards exist, if columns exist, etc.
        """
        data = self.response_dict.get("data", {})
        boards = data.get("boards", [])
        if not boards:
            return (False, "Monday Board doesn't exist")

        columns = boards[0].get("columns", [])
        if not columns:
            return (False, "Monday Board doesn't have columns")

        return (True, "")

    def board_columns(self) -> List[dict]:
        """
        In Go: (s BoardResponse) BoardColumns() []Column
        Return the columns list from the first board.
        """
        data = self.response_dict.get("data", {})
        boards = data.get("boards", [])
        if boards and len(boards) > 0:
            return boards[0].get("columns", [])
        return []


class MondayBoardData:
    """
    Equivalent to the Go 'MondayBoardData' struct:
      Data ItemsBoard `json:"data"`
    """
    def __init__(self, response_dict: dict):
        self.response_dict = response_dict
        data_section = response_dict.get("data", {})
        items_page = data_section.get("items_page_by_column_values", {})

        self.cursor = items_page.get("cursor", "")
        items_raw = items_page.get("items", [])
        self.items = [Item(i) for i in items_raw]

    def has_items(self) -> bool:
        return len(self.items) > 0

    def first(self) -> Optional["Item"]:
        return self.items[0] if self.items else None


class Item:
    """
    Equivalent to the Go 'Item' struct with fields ID, Name, ColumnValues, etc.
    """
    def __init__(self, raw_item: dict):
        self.id = raw_item.get("id", "")
        self.name = raw_item.get("name", "")
        self.column_values = raw_item.get("column_values", [])  # list of dict
        self.map_column_values = {}  # e.g. {column_title -> text}

        self._map_column()

    def _map_column(self):
        """
        Populates self.map_column_values by extracting each column's title => text.
        """
        for col_val in self.column_values:
            column_obj = col_val.get("column", {})
            col_title = column_obj.get("title", "")
            text_val = col_val.get("text", "")
            self.map_column_values[col_title] = text_val

    def column_value(self, title: str) -> str:
        return self.map_column_values.get(title, "")


class Board:
    """
    An interface in Go, but in Python we can define an abstract base class or just a
    concrete class with stubs. We'll define a concrete class, BoardService, below.
    """
    def columns_board(self, board_id: int) -> BoardResponse:
        raise NotImplementedError()


class BoardService(Board):
    """
    The Python equivalent of Go's BoardService struct:
      - Has an HTTPService
      - Provides methods to query/update Monday boards
    """

    BATCH_SIZE_UPDATE = 1
    BATCH_SIZE_FETCH = 25
    FETCH_ALL_LIMIT = 500

    @staticmethod
    def prepare_column_values(columns, board_columns_allowed, input_dict):
        """
        Converts board column definitions + allowed column titles + user input
        into a { column_id: column_value } mapping for Monday.

        :param columns: A list of dicts representing board columns, e.g.:
                        [ {"id": "column_1", "title": "Name"}, {"id": "column_2", "title": "Status"}, ... ]
        :param board_columns_allowed: A list of column titles (strings) we are allowed to update.
        :param input_dict: A dict mapping column title -> the new value (string).
        :return: A dict mapping column_id -> column_value, e.g. {"column_1": "New Name"}
        """
        # 1. Build a map from column title -> column ID
        columns_board_map = {}
        columns_board_type_map = {}
        for col in columns:
            col_title = col.get("title", "")
            col_id = col.get("id", "")
            col_type = col.get("type", "")
            columns_board_map[col_title] = col_id
            columns_board_type_map[col_title] = col_type

        # 2. For each allowed column title, see if it exists in columns_board_map
        #    and if the user input provides a string value for it.
        column_values_to_update = {}
        for col_allowed in board_columns_allowed:
            if col_allowed in columns_board_map:
                col_id = columns_board_map[col_allowed]
                value = input_dict.get(col_allowed)
                # If it's a string, assign it; otherwise ignore it
                if isinstance(value, str):
                    if columns_board_type_map[col_allowed] == "link":
                        column_values_to_update[col_id] = {
                            "url": value,
                            "text": "File link"
                        }
                    else:
                        column_values_to_update[col_id] = value

        return column_values_to_update

    def __init__(self, http_service: HTTP):
        self.http_service = http_service

    def columns_board(self, board_id: int) -> BoardResponse:
        """
        Equivalent to the ColumnsBoard method in Go.
        Fetch columns from a Monday board by ID.
        """
        request_payload = self._query_fetch_columns_from_board(board_id)
        request_json = json.dumps(request_payload).encode("utf-8")

        response_bytes = self.http_service.make_monday_api_call(request_json)
        response_str = response_bytes.decode("utf-8")
        response_dict = json.loads(response_str)

        board_response = BoardResponse(response_dict)
        is_valid, error_msg = board_response.validate()
        if not is_valid:
            raise ValueError(f"response validation failed: {error_msg}")
        return board_response

    # def update_multiple_rows_in_batch(self, board_id: int, updates: List[MondayBoardBatch]) -> None:
    #     """
    #     Equivalent to UpdateMultipleRowsInBatch in Go.
    #     """
    #     total_updates = len(updates)
    #     for i in range(0, total_updates, self.BATCH_SIZE_UPDATE):
    #         end = min(i + self.BATCH_SIZE_UPDATE, total_updates)
    #         batch_updates = updates[i:end]

    #         # Create the mutation query
    #         query_str = self._update_multiple_board_mutation(board_id, batch_updates)
    #         request_json = json.dumps({"query": query_str}).encode("utf-8")

    #         response_bytes = self.http_service.make_monday_api_call(request_json)
    #         logger.info("UpdateMultipleRowsInBatch success, batch %d-%d response: %s",
    #                     i+1, end, response_bytes.decode("utf-8"))

    # def _update_multiple_board_mutation(self, board_id: int, updates: List[MondayBoardBatch]) -> str:
    #     """
    #     Equivalent to UpdateMultipleBoardMutation in Go.
    #     """
    #     mutations = []
    #     for i, update in enumerate(updates):
    #         mutation = (f"update{i+1}: change_multiple_column_values("
    #                     f"item_id: {update.board_monday_id}, "
    #                     f"board_id: {board_id}, "
    #                     f'column_values: "{update.column_values}") {{ id }}')
    #         mutations.append(mutation)
    #     return f"mutation {{{ ' '.join(mutations)} }}"
    def update_multiple_rows_in_batch(self, board_id: int, updates: List[MondayBoardBatch]) -> None:
        """
        Updates multiple rows in a single GraphQL call per batch.
        Uses placeholders ($boardIdX, $itemIdX, $columnValuesX: JSON!) 
        to avoid manually escaping JSON in the mutation string.
        """
        total_updates = len(updates)
        for i in range(0, total_updates, self.BATCH_SIZE_UPDATE):
            end = min(i + self.BATCH_SIZE_UPDATE, total_updates)
            batch_updates = updates[i:end]

            mutation_str, variables = self._update_multiple_board_mutation(board_id, batch_updates)
            payload = {
                "query": mutation_str,
                "variables": variables
            }

            request_json = json.dumps(payload).encode("utf-8")
            response_bytes = self.http_service.make_monday_api_call(request_json)

            logger.info("UpdateMultipleRowsInBatch success, batch %d-%d response: %s",
                        i+1, end, response_bytes.decode("utf-8"))
    
    def _update_multiple_board_mutation(self, board_id: int, updates: List[MondayBoardBatch]) -> tuple:
        """
        Returns (mutation_str, variables_dict) for a single GraphQL batch update.

        Each update is declared as: 
        ($boardIdX: ID!, $itemIdX: ID!, $columnValuesX: JSON!)

        The mutation snippet:
        updateX: change_multiple_column_values(board_id: $boardIdX, item_id: $itemIdX, column_values: $columnValuesX) { id }
        """
        mutations = []
        variables = {}

        for idx, update in enumerate(updates, start=1):
            board_id_var = f"boardId{idx}"
            item_id_var = f"itemId{idx}"
            col_values_var = f"columnValues{idx}"

            # Build one snippet for each row update
            snippet = (
                f"update{idx}: change_multiple_column_values("
                f"board_id: ${board_id_var}, "
                f"item_id: ${item_id_var}, "
                f"column_values: ${col_values_var}"
                f") {{ id }}"
            )
            mutations.append(snippet)

            # Fill in the variables
            variables[board_id_var] = str(board_id)
            variables[item_id_var] = str(update.board_monday_id)

            # If update.column_values is a dict or raw string, 
            # handle escaping. Usually you'd do:
            #   column_vals_str = self._ensure_json_string(update.column_values)
            # But for brevity, assume it's already a properly escaped string or JSON dict:
            variables[col_values_var] = update.column_values

        # Build the top-level mutation definition
        # e.g. mutation ($boardId1: ID!, $itemId1: ID!, $columnValues1: JSON!, $boardId2: ID!, ...) { ... }
        variable_declarations = []
        for key in variables.keys():
            if key.startswith("boardId"):
                variable_declarations.append(f"${key}: ID!")
            elif key.startswith("itemId"):
                variable_declarations.append(f"${key}: ID!")
            elif key.startswith("columnValues"):
                variable_declarations.append(f"${key}: JSON!")

        params_str = ", ".join(variable_declarations)
        body_str = " ".join(mutations)
        mutation_str = f"mutation ({params_str}) {{ {body_str} }}"

        return mutation_str, variables

    def monday_board_rows(self, board_id: int, ids: List[str]) -> MondayBoardData:
        """
        Equivalent to MondayBoardRows in Go.
        Fetch rows matching a set of item 'ids' in batches.
        """
        # For simplicity, skipping the "count" logic from the Go code. We can replicate if needed.
        all_board_rows = []
        cursor = ""

        for i in range(0, len(ids), self.BATCH_SIZE_FETCH):
            end = min(i + self.BATCH_SIZE_FETCH, len(ids))
            batch_ids = ids[i:end]
            request_payload = self._query_fetch_board_rows(board_id, batch_ids, cursor, end)
            request_json = json.dumps(request_payload).encode("utf-8")

            response_bytes = self.http_service.make_monday_api_call(request_json)
            response_str = response_bytes.decode("utf-8")
            response_dict = json.loads(response_str)

            batch_data = MondayBoardData(response_dict)
            all_board_rows.extend(batch_data.items)
            logger.debug("monday_board_rows batch %d-%d response: %s", i+1, end, batch_ids)

            cursor = batch_data.cursor

        # Combine all results into a single MondayBoardData object
        combined_response = {
            "data": {
                "items_page_by_column_values": {
                    "cursor": cursor,
                    "items": [self._item_to_dict(item) for item in all_board_rows]
                }
            }
        }
        return MondayBoardData(combined_response)

    def _query_fetch_columns_from_board(self, board_id: int) -> dict:
        """
        Equivalent to queryFetchColumnsFromBoard(boardID).
        """
        return {
            "query": f"query {{ boards(ids: {board_id}) {{ columns {{ id type title }} }} }}"
        }

    def create_multiple_rows_in_batch(self, board_id: int, creates: List[MondayBoardBatch]) -> None:
        """
        Creates multiple rows in a single GraphQL request per batch.
        We pass column values as a string that contains JSON, matching your successful curl example.
        """
        total_creates = len(creates)
        for i in range(0, total_creates, self.BATCH_SIZE_UPDATE):
            end = min(i + self.BATCH_SIZE_UPDATE, total_creates)
            batch_creates = creates[i:end]

            mutation_str, variables = self._create_multiple_board_mutation(board_id, batch_creates)
            payload = {
                "query": mutation_str,
                "variables": variables
            }

            request_json = json.dumps(payload).encode("utf-8")

            response_bytes = self.http_service.make_monday_api_call(request_json)
            logger.info("CreateMultipleRowsInBatch success, batch %d-%d response: %s",
                        i+1, end, response_bytes.decode("utf-8"))

    def _create_multiple_board_mutation(self, board_id: int, creates: List[MondayBoardBatch]) -> tuple:
        """
        Builds a single mutation + variables dictionary for creating multiple items.
        
        Each item is defined with placeholders:
        ($boardIdX: ID!, $itemNameX: String!, $columnValuesX: JSON!)
        
        And we pass 'columnValuesX' as a *string* that has embedded JSON.
        """
        mutations = []
        variables = {}

        for idx, create in enumerate(creates, start=1):
            board_id_var = f"boardId{idx}"
            item_name_var = f"itemName{idx}"
            col_values_var = f"columnValues{idx}"

            # Build the snippet for the final GraphQL query
            mutation_snippet = (
                f"create{idx}: create_item("
                f"board_id: ${board_id_var}, "
                f"item_name: ${item_name_var}, "
                f"column_values: ${col_values_var}"
                f") {{ id }}"
            )
            mutations.append(mutation_snippet)

            # 1) Board ID as string (Monday expects ID! but often also accepts it as a string)
            variables[board_id_var] = str(board_id)

            # 2) The item (row) name
            variables[item_name_var] = create.item_name

            # 3) The JSON for column values, but we store it as a *string* in 'variables' 
            #    so that the final request includes "columnValuesX":"{ \"key\":\"val\" }"
            # col_values_str = self._ensure_json_string(create.column_values)
            # variables[col_values_var] = col_values_str
            variables[col_values_var] = create.column_values

        # Build the top-level mutation definition with placeholders
        # e.g. mutation ($boardId1: ID!, $itemName1: String!, $columnValues1: JSON!, $boardId2: ID!, ...)
        variable_declarations = []
        for key in variables.keys():
            if key.startswith("boardId"):
                variable_declarations.append(f"${key}: ID!")
            elif key.startswith("itemName"):
                variable_declarations.append(f"${key}: String!")
            elif key.startswith("columnValues"):
                variable_declarations.append(f"${key}: JSON!")

        params_str = ", ".join(variable_declarations)
        mutations_str = " ".join(mutations)

        mutation_str = f"mutation ({params_str}) {{ {mutations_str} }}"

        return mutation_str, variables

    # def _ensure_json_string(self, val: Any) -> str:
    #     """
    #     If 'val' is already a string, assume it's properly escaped JSON.
    #     If 'val' is a dict, convert it to a JSON string so Monday can parse it.
    #     """
    #     if isinstance(val, dict):
    #         # Convert dict -> JSON string
    #         return json.dumps(val)
    #     elif isinstance(val, str):
    #         # Try parsing as JSON:
    #         try:
    #             json.loads(val)
    #             # If this works, val is valid JSON. Return as-is.
    #             return val
    #         except json.JSONDecodeError:
    #             # If parse fails, wrap the string with quotes
    #             return json.dumps(val)
    #     else:
    #         # Fallback: convert to string
    #         return json.dumps(str(val))


    def _query_fetch_board_rows(self, board_id: int, ids: List[str], cursor: str, limit: int) -> dict:
        """
        Equivalent to queryFetchBoardRows in Go.
        """
        # ids_json = json.dumps(ids)

        cursor_part = f', cursor: "{cursor}"' if cursor else ""
        query_str = f"""
            query GetBoardRows($boardId: ID!, $columnId: String!, $ids:[String]!, $limit: Int!) {{
                items_page_by_column_values (
                    board_id: $boardId,
                    columns: [{{column_id: $columnId, column_values: $ids}}],
                    limit: $limit{cursor_part}
                ) {{
                    cursor
                    items {{
                        id
                        name
                        column_values {{
                            column {{
                                title
                            }}
                            text
                        }}
                    }}
                }}
            }}
        """

        return {
            "query": query_str,
            "variables": {
                "boardId": board_id,
                "columnId": "name",
                "ids": ids,
                "limit": limit
            }
        }

    def _item_to_dict(self, item_obj: Item) -> dict:
        """
        Convert an Item object back to a dict so we can combine multiple fetches 
        in the final MondayBoardData aggregator.
        """
        # Recreate the structure that MondayBoardData expects:
        columns_list = []
        for title, text in item_obj.map_column_values.items():
            columns_list.append({
                "column": {"title": title},
                "text": text
            })

        return {
            "id": item_obj.id,
            "name": item_obj.name,
            "column_values": columns_list
        }
