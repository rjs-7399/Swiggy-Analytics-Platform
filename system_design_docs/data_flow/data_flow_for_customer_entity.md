# Customer Entity: Data Flow Operations

## Insert Operation

When a new customer record is inserted, it flows through the multi-layer architecture with streams capturing the changes at each step.

### Step 1: Data Source to Bronze (Stage Layer)


**Example Data in Bronze:**
```
customerid: "101"
name: "John Smith"
mobile: "9876543210"
email: "john@example.com"
loginbyusing: "email"
gender: "male"
dob: "1985-03-15"
anniversary: "2010-06-22"
preferences: "{\"cuisine\": [\"Italian\", \"Chinese\"]}"
createddate: "2022-12-01T09:45:30"
modifieddate: "2022-12-01T09:45:30"
_stg_file_name: "new_customers.csv"
_stg_file_load_ts: 2023-01-15T10:20:30
_stg_file_md5: "a1b2c3d4e5f6g7h8i9j0"
_copy_data_ts: 2023-01-15T10:25:15
```

**Stream Status:**
- New records appear in `stage_sch.customer_stm` with:
  - `METADATA$ACTION = 'INSERT'`
  - `METADATA$ISUPDATE = 'FALSE'`
  - `METADATA$ROW_ID` = unique identifier

### Step 2: Bronze to Silver (Clean Layer)

The MERGE statement processes the stream data, transforming text values to appropriate data types:

**Example Data in Silver:**
```
CUSTOMER_SK: 5001                       // AUTO-INCREMENT SURROGATE KEY
CUSTOMER_ID: "101"                      // Business key
NAME: "John Smith"
MOBILE: "9876543210"
EMAIL: "john@example.com" 
LOGIN_BY_USING: "email"
GENDER: "male"
DOB: 1985-03-15                         // Converted to DATE type
ANNIVERSARY: 2010-06-22                 // Converted to DATE type
PREFERENCES: "{\"cuisine\": [\"Italian\", \"Chinese\"]}"
CREATED_DT: 2022-12-01T09:45:30.000Z    // Converted to TIMESTAMP_TZ
MODIFIED_DT: 2022-12-01T09:45:30.000Z   // Converted to TIMESTAMP_TZ
_STG_FILE_NAME: "new_customers.csv"
_STG_FILE_LOAD_TS: 2023-01-15T10:20:30.000Z
_STG_FILE_MD5: "a1b2c3d4e5f6g7h8i9j0"
_COPY_DATA_TS: 2023-01-15T10:25:15.000Z
```

**Stream Status:**
- New records appear in `clean_sch.customer_stm` with:
  - `METADATA$ACTION = 'INSERT'`
  - `METADATA$ISUPDATE = 'FALSE'`
  - `METADATA$ROW_ID` = unique identifier

### Step 3: Silver to Gold (Consumption Layer - SCD Type 2)

The stream from the clean layer feeds into the dimension table with SCD Type 2 implementation:

**Example Data in Gold Dimension:**
```
CUSTOMER_HK: 7839284792                 // Hash-based surrogate key for SCD Type 2
CUSTOMER_ID: "101"                      // Business key from source
NAME: "John Smith"
MOBILE: "9876543210"
EMAIL: "john@example.com"
LOGIN_BY_USING: "email"
GENDER: "male"
DOB: 1985-03-15
ANNIVERSARY: 2010-06-22
PREFERENCES: "{\"cuisine\": [\"Italian\", \"Chinese\"]}"
EFF_START_DATE: 2023-01-15T00:00:00.000Z // When this version became active
EFF_END_DATE: null                      // NULL for current version
IS_CURRENT: true                        // Flag indicating current version
```

### Step 4: Impact on Fact Table 

When a new order is created for this customer, the order_item_fact references the current customer dimension record:

```
ORDER_ITEM_FACT_SK: 12345               // Auto-increment surrogate key
ORDER_ITEM_ID: 5001                     // Business key
ORDER_ID: 1001                          // Business key
CUSTOMER_DIM_KEY: 7839284792            // Foreign key to CUSTOMER_DIM.CUSTOMER_HK
CUSTOMER_ADDRESS_DIM_KEY: 4537291       // Other dimension foreign keys
...
QUANTITY: 2                             // Measures
PRICE: 15.99
SUBTOTAL: 31.98
```

## Update Operation

When a customer record is updated, streams track the changes and implement SCD Type 2 versioning.

### Step 1: Data Source to Bronze (Stage Layer)

**Example Updated Data in Bronze:**
```
customerid: "101"                       // Same ID
name: "John Smith"                      // Unchanged
mobile: "9876543210"                    // Unchanged
email: "john.doe@example.com"           // CHANGED
loginbyusing: "email"                   // Unchanged
gender: "male"                          // Unchanged
dob: "1985-03-15"                       // Unchanged
anniversary: "2010-06-22"               // Unchanged
preferences: "{\"cuisine\": [\"Italian\", \"Chinese\"]}" // Unchanged
createddate: "2022-12-01T09:45:30"      // Unchanged
modifieddate: "2023-01-20T14:30:22"     // CHANGED
_stg_file_name: "customer_updates.csv"
_stg_file_load_ts: 2023-01-20T14:35:10
_stg_file_md5: "z9y8x7w6v5u4t3s2r1"
_copy_data_ts: 2023-01-20T14:40:05
```

**Stream Status:**
- Records appear in `stage_sch.customer_stm` with:
  - `METADATA$ACTION = 'INSERT'`
  - `METADATA$ISUPDATE = 'FALSE'`
  - Contains the new record with updated values

### Step 2: Bronze to Silver (Clean Layer)

The MERGE statement updates the existing record:

**Example Updated Data in Silver:**
```
CUSTOMER_SK: 5001                       // Same surrogate key
CUSTOMER_ID: "101"                      // Same business key 
NAME: "John Smith"
MOBILE: "9876543210"
EMAIL: "john.doe@example.com"           // CHANGED
LOGIN_BY_USING: "email"
GENDER: "male"
DOB: 1985-03-15
ANNIVERSARY: 2010-06-22
PREFERENCES: "{\"cuisine\": [\"Italian\", \"Chinese\"]}"
CREATED_DT: 2022-12-01T09:45:30.000Z    // Unchanged
MODIFIED_DT: 2023-01-20T14:30:22.000Z   // CHANGED
_STG_FILE_NAME: "customer_updates.csv"  // CHANGED
_STG_FILE_LOAD_TS: 2023-01-20T14:35:10  // CHANGED
_STG_FILE_MD5: "z9y8x7w6v5u4t3s2r1"     // CHANGED
_COPY_DATA_TS: 2023-01-20T14:40:05      // CHANGED
```

**Stream Status:**
- Updated records appear in `clean_sch.customer_stm` with:
  - `METADATA$ACTION = 'DELETE'` (for old version)
  - `METADATA$ISUPDATE = 'TRUE'`
  - `METADATA$ACTION = 'INSERT'` (for new version)
  - `METADATA$ISUPDATE = 'TRUE'`

### Step 3: Silver to Gold (Consumption Layer - SCD Type 2)

The SCD Type 2 process handles the update in two parts:


**Example Data in Gold Dimension - Old Version:**
```
CUSTOMER_HK: 7839284792                 // Original hash key
CUSTOMER_ID: "101"                      // Same business key
NAME: "John Smith"
MOBILE: "9876543210"
EMAIL: "john@example.com"               // Original email
LOGIN_BY_USING: "email"
GENDER: "male"
DOB: 1985-03-15
ANNIVERSARY: 2010-06-22
PREFERENCES: "{\"cuisine\": [\"Italian\", \"Chinese\"]}"
EFF_START_DATE: 2023-01-15T00:00:00     // Original effective date
EFF_END_DATE: 2023-01-20T14:40:05       // Now has an end date - CHANGED
IS_CURRENT: false                       // No longer current - CHANGED
```

**Example Data in Gold Dimension - New Version:**
```
CUSTOMER_HK: 9824563781                 // NEW hash key
CUSTOMER_ID: "101"                      // Same business key
NAME: "John Smith"
MOBILE: "9876543210"
EMAIL: "john.doe@example.com"           // Updated email
LOGIN_BY_USING: "email"
GENDER: "male"
DOB: 1985-03-15
ANNIVERSARY: 2010-06-22
PREFERENCES: "{\"cuisine\": [\"Italian\", \"Chinese\"]}"
EFF_START_DATE: 2023-01-20T14:40:05     // New start date
EFF_END_DATE: null                      // Null end date (current version)
IS_CURRENT: true                        // Current version flag
```

### Step 4: Impact on Fact Table

**Historical Orders (before update):** Continue to point to the original customer dimension record (providing point-in-time accuracy)
```
ORDER_ITEM_FACT (Gold - Fact)
- ORDER_ITEM_FACT_SK: 12345              
- ORDER_ITEM_ID: 5001                    
- ORDER_ID: 1001                         
- CUSTOMER_DIM_KEY: 7839284792           // Points to ORIGINAL customer record
- CUSTOMER_ADDRESS_DIM_KEY: 4537291      
- RESTAURANT_DIM_KEY: 5437218            
- RESTAURANT_LOCATION_DIM_KEY: 6529184   
- MENU_DIM_KEY: 6281937                  
- DELIVERY_AGENT_DIM_KEY: 8361054        
- ORDER_DATE_DIM_KEY: 20230115           
- QUANTITY: 2                            
- PRICE: 15.99                           
- SUBTOTAL: 31.98                        
- DELIVERY_STATUS: "Delivered"           
- ESTIMATED_TIME: "30 minutes"           
```

**New Orders (after update):** Will use the new customer dimension record
```
ORDER_ITEM_FACT (Gold - Fact)
- ORDER_ITEM_FACT_SK: 12389             
- ORDER_ITEM_ID: 5001                    
- ORDER_ID: 1001                         
- CUSTOMER_DIM_KEY: 9824563781           // Points to Updated customer record
- CUSTOMER_ADDRESS_DIM_KEY: 4537291      
- RESTAURANT_DIM_KEY: 5437218            
- RESTAURANT_LOCATION_DIM_KEY: 6529184   
- MENU_DIM_KEY: 6281937                  
- DELIVERY_AGENT_DIM_KEY: 8361054        
- ORDER_DATE_DIM_KEY: 20230115           
- QUANTITY: 2                            
- PRICE: 15.99                           
- SUBTOTAL: 31.98                        
- DELIVERY_STATUS: "Delivered"           
- ESTIMATED_TIME: "30 minutes"           
```

## Delete Operation

Logical deletes in a data warehouse typically involve flagging records rather than physical deletion.

### Step 1: Data Source to Bronze (Stage Layer)


**Example Deletion Data:**
```
customerid: "101"
deletion_reason: "Customer Request"
deletion_timestamp: "2023-02-15T16:30:45"
_stg_file_name: "customer_deletions.csv"
_stg_file_load_ts: 2023-02-15T16:35:20
_stg_file_md5: "q1w2e3r4t5y6u7i8"
_copy_data_ts: 2023-02-15T16:40:10
```

**Stream Status:**
- Deletion records appear in the deletions stream with:
  - `METADATA$ACTION = 'INSERT'`
  - `METADATA$ISUPDATE = 'FALSE'`

### Step 2: Process Deletion in Silver (Clean Layer)

**Example Updated Data in Silver:**
```
CUSTOMER_SK: 5001
CUSTOMER_ID: "101"
NAME: "John Smith"
MOBILE: "9876543210"
EMAIL: "john.doe@example.com"
LOGIN_BY_USING: "email"
GENDER: "male"
DOB: 1985-03-15
ANNIVERSARY: 2010-06-22
PREFERENCES: "{\"cuisine\": [\"Italian\", \"Chinese\"]}"
IS_DELETED: TRUE                        // CHANGED to mark as deleted
DELETION_REASON: "Customer Request"     // ADDED
DELETION_TIMESTAMP: 2023-02-15T16:30:45 // ADDED
CREATED_DT: 2022-12-01T09:45:30.000Z
MODIFIED_DT: 2023-02-15T16:40:10.000Z   // UPDATED
```

**Stream Status:**
- Updated records appear in `clean_sch.customer_stm` with:
  - `METADATA$ACTION = 'DELETE'` (for previous version)
  - `METADATA$ISUPDATE = 'TRUE'`
  - `METADATA$ACTION = 'INSERT'` (for new version with deletion flag)
  - `METADATA$ISUPDATE = 'TRUE'`

### Step 3: Propagate Deletion to Gold (Consumption Layer - SCD Type 2)

**Example Data in Gold Dimension - Final Version (with deletion):**
```
CUSTOMER_HK: 9824563781
CUSTOMER_ID: "101"
NAME: "John Smith"
MOBILE: "9876543210"
EMAIL: "john.doe@example.com"
LOGIN_BY_USING: "email"
GENDER: "male"
DOB: 1985-03-15
ANNIVERSARY: 2010-06-22
PREFERENCES: "{\"cuisine\": [\"Italian\", \"Chinese\"]}"
EFF_START_DATE: 2023-01-20T14:40:05
EFF_END_DATE: 2023-02-15T16:30:45       // Updated with deletion timestamp
IS_CURRENT: false                       // No longer current
IS_DELETED: true                        // Marked as deleted
DELETION_REASON: "Customer Request"     // Deletion reason added
```

**Note:** In this implementation, we don't insert a new version for deleted records, just expire the current one.

### Step 4: Impact on Fact Table

The fact table maintains historical accuracy:

- Order items before deletion continue to point to appropriate historical customer versions
- No new orders should be created for deleted customers (enforced by application logic)

## Stream Status Summary

The stream objects play a crucial role in tracking changes through the ETL pipeline:

1. **Bronze Stage Stream `stage_sch.customer_stm`:**
   - Tracks new data loaded into the bronze layer
   - `METADATA$ACTION = 'INSERT'` for all new records
   - `METADATA$ISUPDATE = 'FALSE'` as it's an append-only stream

2. **Silver Clean Stream `clean_sch.customer_stm`:**
   - Tracks both inserts and updates in the clean layer
   - For inserts: `METADATA$ACTION = 'INSERT'`, `METADATA$ISUPDATE = 'FALSE'`
   - For updates: 
     - Old version: `METADATA$ACTION = 'DELETE'`, `METADATA$ISUPDATE = 'TRUE'`
     - New version: `METADATA$ACTION = 'INSERT'`, `METADATA$ISUPDATE = 'TRUE'`

3. **Gold Consumption Layer:**
   - Uses silver stream to implement SCD Type 2 logic
   - Fact tables reference appropriate dimension versions based on effective dates
