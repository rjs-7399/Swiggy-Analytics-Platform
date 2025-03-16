# Data Flow for Update Operations in the Swiggy Data Warehouse

I'll explain how updates flow through the system for each entity, showing how SCD Type 2 handles historical changes in the dimension tables.

## 1. Customer Dimension: Update Operation Flow

Let's trace a customer update through the system:

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
```

**Update Data (CSV):**
```
101,Rahul Sharma,8765432109,rahul.sharma@gmail.com,Google,Male,1992-05-15,2018-09-22,{"preferences":["North Indian","Chinese","Italian"]},2023-01-10T09:30:45,2023-02-15T14:22:30
```

**Flow Steps:**

1. **Stage/Bronze Layer:**
   - Updated data loaded as raw text
   ```
   customerid: "101"
   name: "Rahul Sharma"
   mobile: "8765432109"  // CHANGED
   email: "rahul.sharma@gmail.com"
   loginbyusing: "Google"
   gender: "Male"
   dob: "1992-05-15"
   anniversary: "2018-09-22"
   preferences: "{"preferences":["North Indian","Chinese","Italian"]}"  // CHANGED
   createddate: "2023-01-10T09:30:45"
   modifieddate: "2023-02-15T14:22:30"  // CHANGED
   ```

2. **Clean/Silver Layer:**
   - Data type conversions applied to the update
   ```
   CUSTOMER_SK: 5001  // Same SK as before
   CUSTOMER_ID: "101"
   NAME: "Rahul Sharma"
   MOBILE: "8765432109"  // CHANGED
   EMAIL: "rahul.sharma@gmail.com"
   LOGIN_BY_USING: "Google"
   GENDER: "Male"
   DOB: 1992-05-15
   ANNIVERSARY: 2018-09-22
   PREFERENCES: {"preferences":["North Indian","Chinese","Italian"]}  // CHANGED
   CREATED_DT: 2023-01-10T09:30:45
   MODIFIED_DT: 2023-02-15T14:22:30  // CHANGED
   ```

3. **Consumption/Gold Layer (SCD Type 2):**
   - Original record is expired
   - New version is created

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
   EFF_END_DATE: 2023-02-15T14:22:30  // Changed from null to update timestamp
   IS_CURRENT: false  // Changed from true to false
   ```

   **New Record (Current Version):**
   ```
   CUSTOMER_HK: 8923457610  // New hash key generated
   CUSTOMER_ID: "101"  // Same business key
   NAME: "Rahul Sharma"
   MOBILE: "8765432109"  // CHANGED
   EMAIL: "rahul.sharma@gmail.com"
   LOGIN_BY_USING: "Google"
   GENDER: "Male"
   DOB: 1992-05-15
   ANNIVERSARY: 2018-09-22
   PREFERENCES: {"preferences":["North Indian","Chinese","Italian"]}  // CHANGED
   EFF_START_DATE: 2023-02-15T14:22:30  // New start date = old end date
   EFF_END_DATE: null
   IS_CURRENT: true
   ```

## 2. Customer Address Dimension: Update Operation Flow

**Original Record in Gold/Consumption Layer:**
```
CUSTOMER_ADDRESS_HK: 8365219047
ADDRESS_ID: 201
CUSTOMER_ID_FK: 101
FLAT_NO: "A-42"
HOUSE_NO: null
FLOOR: "3rd"
BUILDING: "Sunshine Apartments"
LANDMARK: "Near Metro Station"
LOCALITY: "Koramangala"
CITY: "Bangalore"
STATE: "Karnataka"
PINCODE: "560034"
COORDINATES: "12.9352|77.6245"
PRIMARY_FLAG: "Y"
ADDRESS_TYPE: "Home"
EFF_START_DATE: 2023-01-10T10:15:30
EFF_END_DATE: null
IS_CURRENT: true
```

**Update Data (CSV):**
```
201,101,A-42,null,3rd,Sunshine Apartments,Near Metro Station,Whitefield,Bangalore,Karnataka,560066,12.9702|77.7499,Y,Home,2023-01-10T10:15:30,2023-02-20T11:05:45
```

**Flow Steps:**

1. **Stage/Bronze Layer:**
   - Updated data loaded as raw text
   ```
   addressid: "201"
   customerid: "101"
   flatno: "A-42"
   houseno: null
   floor: "3rd"
   building: "Sunshine Apartments"
   landmark: "Near Metro Station"
   locality: "Whitefield"  // CHANGED
   city: "Bangalore"
   state: "Karnataka"
   pincode: "560066"  // CHANGED
   coordinates: "12.9702|77.7499"  // CHANGED
   primaryflag: "Y"
   addresstype: "Home"
   createddate: "2023-01-10T10:15:30"
   modifieddate: "2023-02-20T11:05:45"  // CHANGED
   ```

2. **Clean/Silver Layer:**
   - Same address ID with updated fields
   ```
   CUSTOMER_ADDRESS_SK: 3001  // Same surrogate key
   ADDRESS_ID: 201
   CUSTOMER_ID_FK: 101
   FLAT_NO: "A-42"
   HOUSE_NO: null
   FLOOR: "3rd"
   BUILDING: "Sunshine Apartments"
   LANDMARK: "Near Metro Station"
   LOCALITY: "Whitefield"  // CHANGED
   CITY: "Bangalore"
   STATE: "Karnataka"
   PINCODE: "560066"  // CHANGED
   COORDINATES: "12.9702|77.7499"  // CHANGED
   PRIMARY_FLAG: "Y"
   ADDRESS_TYPE: "Home"
   CREATED_DATE: 2023-01-10T10:15:30
   MODIFIED_DATE: 2023-02-20T11:05:45  // CHANGED
   ```

3. **Consumption/Gold Layer (SCD Type 2):**
   - Original record expires
   - New version is created

   **Original Record (Now Expired):**
   ```
   CUSTOMER_ADDRESS_HK: 8365219047
   ADDRESS_ID: 201
   CUSTOMER_ID_FK: 101
   FLAT_NO: "A-42"
   HOUSE_NO: null
   FLOOR: "3rd"
   BUILDING: "Sunshine Apartments"
   LANDMARK: "Near Metro Station"
   LOCALITY: "Koramangala"
   CITY: "Bangalore"
   STATE: "Karnataka"
   PINCODE: "560034"
   COORDINATES: "12.9352|77.6245"
   PRIMARY_FLAG: "Y"
   ADDRESS_TYPE: "Home"
   EFF_START_DATE: 2023-01-10T10:15:30
   EFF_END_DATE: 2023-02-20T11:05:45  // CHANGED from null
   IS_CURRENT: false  // CHANGED from true
   ```

   **New Record (Current Version):**
   ```
   CUSTOMER_ADDRESS_HK: 6729385401  // New hash key
   ADDRESS_ID: 201  // Same business key
   CUSTOMER_ID_FK: 101
   FLAT_NO: "A-42"
   HOUSE_NO: null
   FLOOR: "3rd"
   BUILDING: "Sunshine Apartments"
   LANDMARK: "Near Metro Station"
   LOCALITY: "Whitefield"  // CHANGED
   CITY: "Bangalore"
   STATE: "Karnataka"
   PINCODE: "560066"  // CHANGED
   COORDINATES: "12.9702|77.7499"  // CHANGED
   PRIMARY_FLAG: "Y"
   ADDRESS_TYPE: "Home"
   EFF_START_DATE: 2023-02-20T11:05:45  // New start date
   EFF_END_DATE: null
   IS_CURRENT: true
   ```

## 3. Date Dimension: Update Operation Flow

The date dimension is typically static once populated and rarely undergoes updates. If needed, updates would be handled directly without SCD Type 2, as dates don't change their attributes historically.

## 4. Delivery Agent Dimension: Update Operation Flow

**Original Record in Gold/Consumption Layer:**
```
DELIVERY_AGENT_HK: 5731982064
DELIVERY_AGENT_ID: 301
NAME: "Suresh Kumar"
PHONE: "9876543210"
VEHICLE_TYPE: "Motorcycle"
LOCATION_ID_FK: 5
STATUS: "Active"
GENDER: "Male"
RATING: 4.8
EFF_START_DATE: 2023-01-11T08:00:00
EFF_END_DATE: null
IS_CURRENT: true
```

**Update Data (CSV):**
```
301,Suresh Kumar,9876543210,Bicycle,5,Active,Male,4.7,2023-01-11T08:00:00,2023-02-10T16:45:32
```

**Flow Steps:**

1. **Stage/Bronze Layer:**
   - Updated data loaded as raw text
   ```
   deliveryagentid: "301"
   name: "Suresh Kumar"
   phone: "9876543210"
   vehicletype: "Bicycle"  // CHANGED from Motorcycle
   locationid: "5"
   status: "Active"
   gender: "Male"
   rating: "4.7"  // CHANGED from 4.8
   createddate: "2023-01-11T08:00:00"
   modifieddate: "2023-02-10T16:45:32"  // CHANGED
   ```

2. **Clean/Silver Layer:**
   - Data type conversions applied to the update
   ```
   DELIVERY_AGENT_SK: 2001  // Same surrogate key
   DELIVERY_AGENT_ID: 301
   NAME: "Suresh Kumar"
   PHONE: "9876543210"
   VEHICLE_TYPE: "Bicycle"  // CHANGED
   LOCATION_ID_FK: 5
   STATUS: "Active"
   GENDER: "Male"
   RATING: 4.7  // CHANGED
   CREATED_DT: 2023-01-11T08:00:00
   MODIFIED_DT: 2023-02-10T16:45:32  // CHANGED
   ```

3. **Consumption/Gold Layer (SCD Type 2):**
   - Original record expires
   - New version is created

   **Original Record (Now Expired):**
   ```
   DELIVERY_AGENT_HK: 5731982064
   DELIVERY_AGENT_ID: 301
   NAME: "Suresh Kumar"
   PHONE: "9876543210"
   VEHICLE_TYPE: "Motorcycle"
   LOCATION_ID_FK: 5
   STATUS: "Active"
   GENDER: "Male"
   RATING: 4.8
   EFF_START_DATE: 2023-01-11T08:00:00
   EFF_END_DATE: 2023-02-10T16:45:32  // CHANGED from null
   IS_CURRENT: false  // CHANGED from true
   ```

   **New Record (Current Version):**
   ```
   DELIVERY_AGENT_HK: 3810274956  // New hash key
   DELIVERY_AGENT_ID: 301  // Same business key
   NAME: "Suresh Kumar"
   PHONE: "9876543210"
   VEHICLE_TYPE: "Bicycle"  // CHANGED
   LOCATION_ID_FK: 5
   STATUS: "Active"
   GENDER: "Male"
   RATING: 4.7  // CHANGED
   EFF_START_DATE: 2023-02-10T16:45:32  // New start date
   EFF_END_DATE: null
   IS_CURRENT: true
   ```

## 5. Restaurant Location Dimension: Update Operation Flow

**Original Record in Gold/Consumption Layer:**
```
RESTAURANT_LOCATION_HK: 3641897520
LOCATION_ID: 5
CITY: "Mumbai"
STATE: "Maharashtra"
STATE_CODE: "MH"
IS_UNION_TERRITORY: false
CAPITAL_CITY_FLAG: true
CITY_TIER: "Tier-1"
ZIP_CODE: "400001"
ACTIVE_FLAG: "Y"
EFF_START_DT: 2023-01-05T10:00:00
EFF_END_DT: null
CURRENT_FLAG: true
```

**Update Data (CSV):**
```
5,Mumbai,Maharashtra,400002,Y,2023-01-05T10:00:00,2023-03-01T09:15:00
```

**Flow Steps:**

1. **Stage/Bronze Layer:**
   - Updated data loaded as raw text
   ```
   locationid: "5"
   city: "Mumbai"
   state: "Maharashtra"
   zipcode: "400002"  // CHANGED from 400001
   activeflag: "Y"
   createdate: "2023-01-05T10:00:00"
   modifieddate: "2023-03-01T09:15:00"  // CHANGED
   ```

2. **Clean/Silver Layer:**
   - Data type conversions plus business rule enrichment
   ```
   RESTAURANT_LOCATION_SK: 1001  // Same surrogate key
   LOCATION_ID: 5
   CITY: "Mumbai"
   STATE: "Maharashtra"
   STATE_CODE: "MH"
   IS_UNION_TERRITORY: false
   CAPITAL_CITY_FLAG: true
   CITY_TIER: "Tier-1"
   ZIP_CODE: "400002"  // CHANGED
   ACTIVE_FLAG: "Y"
   CREATED_TS: 2023-01-05T10:00:00
   MODIFIED_TS: 2023-03-01T09:15:00  // CHANGED
   ```

3. **Consumption/Gold Layer (SCD Type 2):**
   - Original record expires
   - New version is created

   **Original Record (Now Expired):**
   ```
   RESTAURANT_LOCATION_HK: 3641897520
   LOCATION_ID: 5
   CITY: "Mumbai"
   STATE: "Maharashtra"
   STATE_CODE: "MH"
   IS_UNION_TERRITORY: false
   CAPITAL_CITY_FLAG: true
   CITY_TIER: "Tier-1"
   ZIP_CODE: "400001"
   ACTIVE_FLAG: "Y"
   EFF_START_DT: 2023-01-05T10:00:00
   EFF_END_DT: 2023-03-01T09:15:00  // CHANGED from null
   CURRENT_FLAG: false  // CHANGED from true
   ```

   **New Record (Current Version):**
   ```
   RESTAURANT_LOCATION_HK: 9472658103  // New hash key
   LOCATION_ID: 5  // Same business key
   CITY: "Mumbai"
   STATE: "Maharashtra"
   STATE_CODE: "MH"
   IS_UNION_TERRITORY: false
   CAPITAL_CITY_FLAG: true
   CITY_TIER: "Tier-1"
   ZIP_CODE: "400002"  // CHANGED
   ACTIVE_FLAG: "Y"
   EFF_START_DT: 2023-03-01T09:15:00  // New start date
   EFF_END_DT: null
   CURRENT_FLAG: true
   ```

## 6. Restaurant Dimension: Update Operation Flow

**Original Record in Gold/Consumption Layer:**
```
RESTAURANT_HK: 9157623048
RESTAURANT_ID: 401
NAME: "Spice Garden"
CUISINE_TYPE: "North Indian|Chinese"
PRICING_FOR_TWO: 800.00
RESTAURANT_PHONE: "9988776655"
OPERATING_HOURS: "10:00-23:00"
LOCATION_ID_FK: 5
ACTIVE_FLAG: "Y"
OPEN_STATUS: "Open"
LOCALITY: "Bandra"
RESTAURANT_ADDRESS: "Mumbai Premium Plaza Bandra West"
LATITUDE: 19.0596
LONGITUDE: 72.8295
EFF_START_DATE: 2023-01-08T12:30:00
EFF_END_DATE: null
IS_CURRENT: true
```

**Update Data (CSV):**
```
401,Spice Garden Deluxe,North Indian|Chinese|Continental,950,9988776655,10:00-23:30,5,Y,Open,Bandra,Mumbai Premium Plaza Bandra West,19.0596,72.8295,2023-01-08T12:30:00,2023-02-25T13:40:12
```

**Flow Steps:**

1. **Stage/Bronze Layer:**
   - Updated data loaded as raw text
   ```
   restaurantid: "401"
   name: "Spice Garden Deluxe"  // CHANGED
   cuisinetype: "North Indian|Chinese|Continental"  // CHANGED
   pricing_for_2: "950"  // CHANGED
   restaurant_phone: "9988776655"
   operatinghours: "10:00-23:30"  // CHANGED
   locationid: "5"
   activeflag: "Y"
   openstatus: "Open"
   locality: "Bandra"
   restaurant_address: "Mumbai Premium Plaza Bandra West"
   latitude: "19.0596"
   longitude: "72.8295"
   createddate: "2023-01-08T12:30:00"
   modifieddate: "2023-02-25T13:40:12"  // CHANGED
   ```

2. **Clean/Silver Layer:**
   - Data type conversions
   ```
   RESTAURANT_SK: 4001  // Same surrogate key
   RESTAURANT_ID: 401
   NAME: "Spice Garden Deluxe"  // CHANGED
   CUISINE_TYPE: "North Indian|Chinese|Continental"  // CHANGED
   PRICING_FOR_TWO: 950.00  // CHANGED
   RESTAURANT_PHONE: "9988776655"
   OPERATING_HOURS: "10:00-23:30"  // CHANGED
   LOCATION_ID_FK: 5
   ACTIVE_FLAG: "Y"
   OPEN_STATUS: "Open"
   LOCALITY: "Bandra"
   RESTAURANT_ADDRESS: "Mumbai Premium Plaza Bandra West"
   LATITUDE: 19.0596
   LONGITUDE: 72.8295
   CREATED_DT: 2023-01-08T12:30:00
   MODIFIED_DT: 2023-02-25T13:40:12  // CHANGED
   ```

3. **Consumption/Gold Layer (SCD Type 2):**
   - Original record expires
   - New version is created

   **Original Record (Now Expired):**
   ```
   RESTAURANT_HK: 9157623048
   RESTAURANT_ID: 401
   NAME: "Spice Garden"
   CUISINE_TYPE: "North Indian|Chinese"
   PRICING_FOR_TWO: 800.00
   RESTAURANT_PHONE: "9988776655"
   OPERATING_HOURS: "10:00-23:00"
   LOCATION_ID_FK: 5
   ACTIVE_FLAG: "Y"
   OPEN_STATUS: "Open"
   LOCALITY: "Bandra"
   RESTAURANT_ADDRESS: "Mumbai Premium Plaza Bandra West"
   LATITUDE: 19.0596
   LONGITUDE: 72.8295
   EFF_START_DATE: 2023-01-08T12:30:00
   EFF_END_DATE: 2023-02-25T13:40:12  // CHANGED from null
   IS_CURRENT: false  // CHANGED from true
   ```

   **New Record (Current Version):**
   ```
   RESTAURANT_HK: 2765834901  // New hash key
   RESTAURANT_ID: 401  // Same business key
   NAME: "Spice Garden Deluxe"  // CHANGED
   CUISINE_TYPE: "North Indian|Chinese|Continental"  // CHANGED
   PRICING_FOR_TWO: 950.00  // CHANGED
   RESTAURANT_PHONE: "9988776655"
   OPERATING_HOURS: "10:00-23:30"  // CHANGED
   LOCATION_ID_FK: 5
   ACTIVE_FLAG: "Y"
   OPEN_STATUS: "Open"
   LOCALITY: "Bandra"
   RESTAURANT_ADDRESS: "Mumbai Premium Plaza Bandra West"
   LATITUDE: 19.0596
   LONGITUDE: 72.8295
   EFF_START_DATE: 2023-02-25T13:40:12  // New start date
   EFF_END_DATE: null
   IS_CURRENT: true
   ```

## 7. Menu Dimension: Update Operation Flow

**Original Record in Gold/Consumption Layer:**
```
MENU_DIM_HK: 6258473910
MENU_ID: 501
RESTAURANT_ID_FK: 401
ITEM_NAME: "Butter Chicken"
DESCRIPTION: "Tender chicken in rich tomato gravy"
PRICE: 350.00
CATEGORY: "Main Course"
AVAILABILITY: true
ITEM_TYPE: "Non-Veg"
EFF_START_DATE: 2023-01-09T11:00:00
EFF_END_DATE: null
IS_CURRENT: true
```

**Update Data (CSV):**
```
501,401,Butter Chicken,Tender chicken in rich tomato and cream gravy,395,Main Course,true,Non-Veg,2023-01-09T11:00:00,2023-03-05T10:25:40
```

**Flow Steps:**

1. **Stage/Bronze Layer:**
   - Updated data loaded as raw text
   ```
   menuid: "501"
   restaurantid: "401"
   itemname: "Butter Chicken"
   description: "Tender chicken in rich tomato and cream gravy"  // CHANGED
   price: "395"  // CHANGED from 350
   category: "Main Course"
   availability: "true"
   itemtype: "Non-Veg"
   createddate: "2023-01-09T11:00:00"
   modifieddate: "2023-03-05T10:25:40"  // CHANGED
   ```

2. **Clean/Silver Layer:**
   - Data type conversions and validations
   ```
   MENU_SK: 6001  // Same surrogate key
   MENU_ID: 501
   RESTAURANT_ID_FK: 401
   ITEM_NAME: "Butter Chicken"
   DESCRIPTION: "Tender chicken in rich tomato and cream gravy"  // CHANGED
   PRICE: 395.00  // CHANGED
   CATEGORY: "Main Course"
   AVAILABILITY: true
   ITEM_TYPE: "Non-Veg"
   CREATED_DT: 2023-01-09T11:00:00
   MODIFIED_DT: 2023-03-05T10:25:40  // CHANGED
   ```

3. **Consumption/Gold Layer (SCD Type 2):**
   - Original record expires
   - New version is created

   **Original Record (Now Expired):**
   ```
   MENU_DIM_HK: 6258473910
   MENU_ID: 501
   RESTAURANT_ID_FK: 401
   ITEM_NAME: "Butter Chicken"
   DESCRIPTION: "Tender chicken in rich tomato gravy"
   PRICE: 350.00
   CATEGORY: "Main Course"
   AVAILABILITY: true
   ITEM_TYPE: "Non-Veg"
   EFF_START_DATE: 2023-01-09T11:00:00
   EFF_END_DATE: 2023-03-05T10:25:40  // CHANGED from null
   IS_CURRENT: false  // CHANGED from true
   ```

   **New Record (Current Version):**
   ```
   MENU_DIM_HK: 5049182736  // New hash key
   MENU_ID: 501  // Same business key
   RESTAURANT_ID_FK: 401
   ITEM_NAME: "Butter Chicken"
   DESCRIPTION: "Tender chicken in rich tomato and cream gravy"  // CHANGED
   PRICE: 395.00  // CHANGED
   CATEGORY: "Main Course"
   AVAILABILITY: true
   ITEM_TYPE: "Non-Veg"
   EFF_START_DATE: 2023-03-05T10:25:40  // New start date
   EFF_END_DATE: null
   IS_CURRENT: true
   ```

## 8. Order Item Fact: Update Flow

Unlike dimensions, the fact table typically doesn't implement SCD Type 2. Instead, updates are handled directly, or in some cases, as corrective inserts with flags.

**Original Record in Fact Table:**
```
ORDER_ITEM_FACT_SK: 8001
ORDER_ITEM_ID: 2001
ORDER_ID: 1001
CUSTOMER_DIM_KEY: 7645382910
CUSTOMER_ADDRESS_DIM_KEY: 8365219047
RESTAURANT_DIM_KEY: 9157623048
RESTAURANT_LOCATION_DIM_KEY: 3641897520
MENU_DIM_KEY: 6258473910
DELIVERY_AGENT_DIM_KEY: 5731982064
ORDER_DATE_DIM_KEY: 9273648501
QUANTITY: 2
PRICE: 350.00
SUBTOTAL: 700.00
DELIVERY_STATUS: "Delivered"
ESTIMATED_TIME: "30 minutes"
```

**Update Data (Delivery Status Change):**
```
2001,1001,501,2,350,700,Refunded,30 minutes,2023-01-15T19:30:00,2023-01-20T11:20:00
```

**Flow Steps:**

1. **Stage/Bronze to Clean/Silver Layers:**
   - Process update through transactional tables

2. **Fact Table Update:**

```
ORDER_ITEM_FACT_SK: 8002  -- New surrogate key
ORDER_ITEM_ID: 2002
ORDER_ID: 1002
CUSTOMER_DIM_KEY: 8923457610  -- References the NEW customer record (after update)
CUSTOMER_ADDRESS_DIM_KEY: 6729385401  -- References the NEW address record (after update)
RESTAURANT_DIM_KEY: 2765834901  -- References the NEW restaurant record (after update)
RESTAURANT_LOCATION_DIM_KEY: 9472658103  -- References the NEW location record (after update)
MENU_DIM_KEY: 5049182736  -- References the NEW menu record (after update)
DELIVERY_AGENT_DIM_KEY: 3810274956  -- References the NEW delivery agent record (after update)
ORDER_DATE_DIM_KEY: 8273648593  -- Different date
QUANTITY: 1
PRICE: 395.00  -- Reflects updated menu price
SUBTOTAL: 395.00
DELIVERY_STATUS: "Delivered"
ESTIMATED_TIME: "25 minutes"
```