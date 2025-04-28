import json
from MondayBoard import BoardService, MondayBoardBatch
from MondayHTTP import HTTPService  # Assuming HttpService is defined in HttpService module
from typing import Dict

class ProductCodeColumns:
    StockNumber = "BB Stock Number"
    FCBHLanguageID = "FCBH Language ID"
    BiblebrainLink = "Biblebrain Link"
    Language = "Language"
    Licensor = "Licensor"
    CoLicensor = "Co-Licensor"
    Version = "Version"
    LanguageCountry = "Language Country"
    Mode = "Mode"

ProductCodeColumnAllowed = [
    ProductCodeColumns.StockNumber,
    ProductCodeColumns.FCBHLanguageID,
    ProductCodeColumns.BiblebrainLink,
    ProductCodeColumns.Language,
    ProductCodeColumns.Licensor,
    ProductCodeColumns.CoLicensor,
    ProductCodeColumns.Version,
    ProductCodeColumns.LanguageCountry,
    ProductCodeColumns.Mode,
]

class MondayProductCodeBoard:
    def __init__(self, config):
        self.board_id = config.monday_completed_product_code_board_id
        self.config = config

    def __str__(self):
        return f"Board: {self.board_id}"     

    # -------------------------------------------------------------------------
    # NEW METHOD: synchronize
    # -------------------------------------------------------------------------

    def synchronize(self, data: Dict[str, Dict[str, str]]):
        """
        Synchronize a set of product codes with Monday.
        
        :param data: Dictionary where:
                     - key = product code (str)
                     - value = dict of column title -> value
                       e.g. {
                         "Stock Number": "SXXD1",
                         "FCBH Language ID": 23242,
                         "Biblebrain Link": "https://code.com.co"
                       }

        Steps:
        1. Extract product code keys (data.keys()).
        2. Fetch existing Monday items for those product codes via monday_board_rows.
        3. Determine which codes already exist vs which are new.
        4. Prepare updates for existing items; prepare creates for new items.
        5. Perform batch update & batch create accordingly.
        """

        http_service = HTTPService(self.config.monday_completed_product_code_api_key, "https://api.monday.com/v2")
        board_service = BoardService(http_service)

        # 1. Extract all product codes
        product_codes = list(data.keys())

        if not product_codes:
            print("No product codes provided; nothing to synchronize.")
            return

        # 2. Fetch the existing Monday items matching those product codes
        #    (assumes Monday item 'name' = product code)
        existing_rows_data = board_service.monday_board_rows(self.board_id, product_codes)
        # existing_rows_data is a MondayBoardData object with .items, each an Item with .name and .id

        # Build a dict { product_code -> item_object }
        existing_dict = {}
        for item_obj in existing_rows_data.items:
            product_code = item_obj.name  # e.g. "ProductCodeKey1"
            existing_dict[product_code] = item_obj

        # 3. We also need to prepare column values. First, fetch board columns
        columns_response = board_service.columns_board(self.board_id)  # returns BoardResponse
        board_columns = columns_response.board_columns()
        # Suppose we have a known set of columns we want to update
        # board_columns_allowed = ["Stock Number", "FCBH Language ID", "Biblebrain Link"]

        # 4. Build the update & create lists
        updates = []
        creates = []

        # MondayBoardBatch is typically (board_monday_id, item_name, column_values)
        # For an update: board_monday_id = existing item ID, item_name = "", column_values = <json>
        # For a create: board_monday_id = 0 or ignored, item_name = product code, column_values = <json>

        for product_code in product_codes:
            row_values = data[product_code]  # e.g. { "Stock Number": "SXXD1", ... }
            col_values_dict = BoardService.prepare_column_values(board_columns, ProductCodeColumnAllowed, row_values)
            col_values_json = json.dumps(col_values_dict)  # JSON string for the column values

            if product_code in existing_dict:
                # This product code already exists => update
                item_id = int(existing_dict[product_code].id)
                updates.append(
                    MondayBoardBatch(
                        board_monday_id=item_id,
                        item_name="",  # not used for update
                        column_values=col_values_json
                    )
                )
            else:
                # New row => create
                creates.append(
                    MondayBoardBatch(
                        board_monday_id=0,         # or any placeholder
                        item_name=product_code,    # name of the new item in Monday
                        column_values=col_values_json
                    )
                )

        # 5. Perform the batch updates and creates
        if updates:
            board_service.update_multiple_rows_in_batch(self.board_id, updates)
        if creates:
            board_service.create_multiple_rows_in_batch(self.board_id, creates)

        print(f"Synchronization complete. {len(updates)} updated, {len(creates)} created.")

        for product_code in product_codes:
            if product_code in existing_dict:
                item_id = existing_dict[product_code].id
                print("Updating existing Monday item: product_code=%s (item_id=%s)" % (product_code, item_id))
            else:
                print("Creating new Monday item: product_code=%s" % product_code)

