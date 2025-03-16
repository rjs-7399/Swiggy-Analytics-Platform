## 1. Customer Dimension: Delete Operation Flow

**Original Record in Gold/Consumption Layer:**
```
CUSTOMER_HK: 7645382910
CUSTOMER_ID: "101"
NAME: "Rahul Sharma"
MOBILE: "9876543210"
EMAIL: "rahul.sharma@gmail.com"
LOGIN_BY_USING: "Google"
GENDER: "Male"
DOB: 1992-05-15
ANNIVERSARY: 2018-09-22
PREFERENCES: {"preferences":["North Indian","Chinese"]}
EFF_START_DATE: 2023-01-10T09:30:45
EFF_END_DATE: null
IS_CURRENT: true
IS_DELETED: false
```

**Delete Request (CSV or API):**
```
customerid: "101"
delete_reason: "Account closed at customer request"
delete_timestamp: "2023-03-10T14:30:00"
```

**Flow Steps:**

1. **Stage/Bronze Layer:**
   - Delete request processed
   ```
   customerid: "101"
   delete_reason: "Account closed at customer request"
   delete_timestamp: "2023-03-10T14:30:00"
   ```

2. **Clean/Silver Layer:**
   - Record marked for deletion
   ```
   CUSTOMER_SK: 5001
   CUSTOMER_ID: "101"
   // Other fields remain the same
   IS_DELETED: true  // Added or updated
   DELETE_REASON: "Account closed at customer request"  // Added
   DELETE_TIMESTAMP: 2023-03-10T14:30:00  // Added
   ```

3. **Consumption/Gold Layer (SCD Type 2):**
   - Existing active record is expired

   **Original Record (Now Expired):**
   ```
   CUSTOMER_HK: 7645382910
   CUSTOMER_ID: "101"
   NAME: "Rahul Sharma"
   MOBILE: "9876543210"
   EMAIL: "rahul.sharma@gmail.com"
   LOGIN_BY_USING: "Google"
   GENDER: "Male"
   DOB: 1992-05-15
   ANNIVERSARY: 2018-09-22
   PREFERENCES: {"preferences":["North Indian","Chinese"]}
   EFF_START_DATE: 2023-01-10T09:30:45
   EFF_END_DATE: 2023-03-10T14:30:00  // Set to deletion timestamp
   IS_CURRENT: false  // Changed from true
   ```

## 2. Customer Address Dimension: Delete Operation Flow

**Original Record in Gold/Consumption Layer:**
```
CUSTOMER_ADDRESS_HK: 8365219047
ADDRESS_ID: 201
CUSTOMER_ID_FK: 101
FLAT_NO: "A-42"
// Other address fields...
EFF_START_DATE: 2023-01-10T10:15:30
EFF_END_DATE: null
IS_CURRENT: true
IS_DELETED: false
```

**Delete Request:**
```
addressid: "201"
delete_reason: "Customer moved"
delete_timestamp: "2023-03-15T09:45:00"
```

**Flow Steps:**

1. **Stage/Bronze Layer:**
   - Delete request processed
   ```
   addressid: "201"
   delete_reason: "Customer moved"
   delete_timestamp: "2023-03-15T09:45:00"
   ```

2. **Clean/Silver Layer:**
   - Record marked for deletion
   ```
   CUSTOMER_ADDRESS_SK: 3001
   ADDRESS_ID: 201
   // Other fields remain the same
   IS_DELETED: true  // Added or updated
   DELETE_REASON: "Customer moved"  // Added
   DELETE_TIMESTAMP: 2023-03-15T09:45:00  // Added
   ```

3. **Consumption/Gold Layer (SCD Type 2):**
   - Existing active record is expired

   **Original Record (Now Expired):**
   ```
   CUSTOMER_ADDRESS_HK: 8365219047
   ADDRESS_ID: 201
   // Other address fields...
   EFF_START_DATE: 2023-01-10T10:15:30
   EFF_END_DATE: 2023-03-15T09:45:00  // Set to deletion timestamp
   IS_CURRENT: false  // Changed from true
   ```

## 3. Date Dimension: Delete Operation Flow

The date dimension is not subject to deletions as dates are fixed reference data.

## 4. Delivery Agent Dimension: Delete Operation Flow

**Original Record in Gold/Consumption Layer:**
```
DELIVERY_AGENT_HK: 5731982064
DELIVERY_AGENT_ID: 301
NAME: "Suresh Kumar"
// Other agent fields...
EFF_START_DATE: 2023-01-11T08:00:00
EFF_END_DATE: null
IS_CURRENT: true
IS_DELETED: false
```

**Delete Request:**
```
deliveryagentid: "301"
delete_reason: "Agent left company"
delete_timestamp: "2023-04-05T17:30:00"
```

**Flow Steps:**

1. **Stage/Bronze Layer:**
   - Delete request processed
   ```
   deliveryagentid: "301"
   delete_reason: "Agent left company"
   delete_timestamp: "2023-04-05T17:30:00"
   ```

2. **Clean/Silver Layer:**
   - Record marked for deletion
   ```
   DELIVERY_AGENT_SK: 2001
   DELIVERY_AGENT_ID: 301
   // Other fields remain the same
   IS_DELETED: true  // Added or updated
   DELETE_REASON: "Agent left company"  // Added
   DELETE_TIMESTAMP: 2023-04-05T17:30:00  // Added
   ```

3. **Consumption/Gold Layer (SCD Type 2):**
   - Existing active record is expired

   **Original Record (Now Expired):**
   ```
   DELIVERY_AGENT_HK: 5731982064
   DELIVERY_AGENT_ID: 301
   NAME: "Suresh Kumar"
   // Other agent fields...
   EFF_START_DATE: 2023-01-11T08:00:00
   EFF_END_DATE: 2023-04-05T17:30:00  // Set to deletion timestamp
   IS_CURRENT: false  // Changed from true
   ```

## 5. Restaurant Location Dimension: Delete Operation Flow

**Original Record in Gold/Consumption Layer:**
```
RESTAURANT_LOCATION_HK: 3641897520
LOCATION_ID: 5
CITY: "Mumbai"
// Other location fields...
EFF_START_DT: 2023-01-05T10:00:00
EFF_END_DT: null
CURRENT_FLAG: true
IS_DELETED: false
```

**Delete Request:**
```
locationid: "5"
delete_reason: "Location no longer serviced"
delete_timestamp: "2023-05-01T11:20:00"
```

**Flow Steps:**

1. **Stage/Bronze Layer:**
   - Delete request processed
   ```
   locationid: "5"
   delete_reason: "Location no longer serviced"
   delete_timestamp: "2023-05-01T11:20:00"
   ```

2. **Clean/Silver Layer:**
   - Record marked for deletion
   ```
   RESTAURANT_LOCATION_SK: 1001
   LOCATION_ID: 5
   // Other fields remain the same
   IS_DELETED: true  // Added or updated
   DELETE_REASON: "Location no longer serviced"  // Added
   DELETE_TIMESTAMP: 2023-05-01T11:20:00  // Added
   ```

3. **Consumption/Gold Layer (SCD Type 2):**
   - Existing active record is expired

   **Original Record (Now Expired):**
   ```
   RESTAURANT_LOCATION_HK: 3641897520
   LOCATION_ID: 5
   CITY: "Mumbai"
   // Other location fields...
   EFF_START_DT: 2023-01-05T10:00:00
   EFF_END_DT: 2023-05-01T11:20:00  // Set to deletion timestamp
   IS_CURRENT: false  // Changed from true
   IS_DELETED: false  // Unchanged
   ```

## 6. Restaurant Dimension: Delete Operation Flow

**Original Record in Gold/Consumption Layer:**
```
RESTAURANT_HK: 9157623048
RESTAURANT_ID: 401
NAME: "Spice Garden"
// Other restaurant fields...
EFF_START_DATE: 2023-01-08T12:30:00
EFF_END_DATE: null
IS_CURRENT: true
IS_DELETED: false
```

**Delete Request:**
```
restaurantid: "401"
delete_reason: "Restaurant closed permanently"
delete_timestamp: "2023-06-10T18:00:00"
```

**Flow Steps:**

1. **Stage/Bronze Layer:**
   - Delete request processed
   ```
   restaurantid: "401"
   delete_reason: "Restaurant closed permanently"
   delete_timestamp: "2023-06-10T18:00:00"
   ```

2. **Clean/Silver Layer:**
   - Record marked for deletion
   ```
   RESTAURANT_SK: 4001
   RESTAURANT_ID: 401
   // Other fields remain the same
   IS_DELETED: true  // Added or updated
   DELETE_REASON: "Restaurant closed permanently"  // Added
   DELETE_TIMESTAMP: 2023-06-10T18:00:00  // Added
   ```

3. **Consumption/Gold Layer (SCD Type 2):**
   - Existing active record is expired

   **Original Record (Now Expired):**
   ```
   RESTAURANT_HK: 9157623048
   RESTAURANT_ID: 401
   NAME: "Spice Garden"
   // Other restaurant fields...
   EFF_START_DATE: 2023-01-08T12:30:00
   EFF_END_DATE: 2023-06-10T18:00:00  // Set to deletion timestamp
   IS_CURRENT: false  // Changed from true
   ```


## 7. Menu Dimension: Delete Operation Flow

**Original Record in Gold/Consumption Layer:**
```
MENU_DIM_HK: 6258473910
MENU_ID: 501
RESTAURANT_ID_FK: 401
ITEM_NAME: "Butter Chicken"
// Other menu fields...
EFF_START_DATE: 2023-01-09T11:00:00
EFF_END_DATE: null
IS_CURRENT: true
IS_DELETED: false
```

**Delete Request:**
```
menuid: "501"
delete_reason: "Item discontinued"
delete_timestamp: "2023-06-15T12:40:00"
```

**Flow Steps:**

1. **Stage/Bronze Layer:**
   - Delete request processed
   ```
   menuid: "501"
   delete_reason: "Item discontinued"
   delete_timestamp: "2023-06-15T12:40:00"
   ```

2. **Clean/Silver Layer:**
   - Record marked for deletion
   ```
   MENU_SK: 6001
   MENU_ID: 501
   // Other fields remain the same
   IS_DELETED: true  // Added or updated
   DELETE_REASON: "Item discontinued"  // Added
   DELETE_TIMESTAMP: 2023-06-15T12:40:00  // Added
   ```

3. **Consumption/Gold Layer (SCD Type 2):**
   - Existing active record is expired

   **Original Record (Now Expired):**
   ```
   MENU_DIM_HK: 6258473910
   MENU_ID: 501
   RESTAURANT_ID_FK: 401
   ITEM_NAME: "Butter Chicken"
   // Other menu fields...
   EFF_START_DATE: 2023-01-09T11:00:00
   EFF_END_DATE: 2023-06-15T12:40:00  // Set to deletion timestamp
   IS_CURRENT: false  // Changed from true
   ```

## 8. Order Item Fact: Delete Operation Flow

For fact tables, there are two primary approaches to deletion:

### Approach 1: Logical Delete (Most Common)

**Original Fact Record:**
```
ORDER_ITEM_FACT_SK: 8001
ORDER_ITEM_ID: 2001
ORDER_ID: 1001
// Other dimension references and measures...
DELIVERY_STATUS: "Delivered"
IS_DELETED: false
```

**Delete Request:**
```
orderitemid: "2001"
delete_reason: "Order returned and refunded"
delete_timestamp: "2023-03-25T15:10:00"
```

**Flow for Logical Delete:**
1. **Update fact record with deletion flag:**
   ```
   ORDER_ITEM_FACT_SK: 8001  // Unchanged
   ORDER_ITEM_ID: 2001
   ORDER_ID: 1001
   // Other fields remain the same
   DELIVERY_STATUS: "Returned"  // Might be updated
   IS_DELETED: true  // Changed to true
   DELETE_REASON: "Order returned and refunded"  // Added
   DELETE_TIMESTAMP: 2023-03-25T15:10:00  // Added
   ```

2. **Query Impact:** Queries must filter IS_DELETED = false for normal reporting

### Approach 2: Physical Deletion (Less Common)

In some regulated environments (like GDPR requests), physical deletion might be required:

1. **Before Deletion:**
   ```
   // Record exists in fact table
   ```

2. **After Deletion:**
   ```
   // Record completely removed from the table
   ```

3. **Audit Trail:** A separate audit table might record that a deletion occurred, without storing the specific data that was deleted:
   ```
   DELETION_AUDIT_ID: 12345
   TABLE_NAME: "ORDER_ITEM_FACT"
   PRIMARY_KEY_VALUE: "8001"
   DELETE_REASON: "GDPR Request"
   DELETE_TIMESTAMP: 2023-03-25T15:10:00
   DELETED_BY: "System"
   ```

## Key Points About Deletion Flow

1. **Logical vs. Physical Deletion:**
   - Most data warehouses use logical deletion (setting flags) rather than physical removal
   - This preserves the historical record while allowing "deleted" data to be filtered out of reports
   - Physical deletion is typically reserved for regulatory compliance requirements

2. **Dimension Tables (SCD Type 2):**
   - Similar to updates, deletions often result in a new version with a deletion flag
   - The previous version is expired by setting its end date and turning off its current flag
   - This approach maintains referential integrity with fact tables

3. **Fact Tables:**
   - Usually implement logical deletion with flags
   - Foreign keys to dimensions are maintained for historical accuracy
   - Queries filter out deleted records for standard reporting

4. **Impact on Business Intelligence:**
   - Reports and dashboards typically filter out logically deleted records by default
   - Historical analysis may include or exclude deleted records depending on business needs
   - Audit reports can specifically target deleted records for compliance or investigation

5. **Implementation Details:**
   - Deletion flags may include supplementary information like reason codes and timestamps
   - Automated processes might archive or physically remove older deleted records after a retention period
   - Some systems implement "soft delete" with capabilities to "undelete" when appropriate

6. **Best Practices:**
   - Always include deletion timestamps to maintain a timeline
   - Document deletion reasons for audit purposes
   - Implement proper security around deletion operations
   - Consider regulatory requirements when designing deletion processes

This approach ensures that the data warehouse maintains historical accuracy while accommodating the need to remove or mask certain data over time.