# 1. Customer Dimension: Insert Operation Flow

Let's trace a new customer record through the system:

**Source Data (CSV):**
```
101,Rahul Sharma,9876543210,rahul.sharma@gmail.com,Google,Male,1992-05-15,2018-09-22,{"preferences":["North Indian","Chinese"]},2023-01-10T09:30:45,2023-01-10T09:30:45
```

**Flow Steps:**

1. **Stage/Bronze Layer:**
   - Data loaded as raw text to preserve original format
   ```
   customerid: "101"
   name: "Rahul Sharma"
   mobile: "9876543210"
   email: "rahul.sharma@gmail.com"
   loginbyusing: "Google"
   gender: "Male"
   dob: "1992-05-15"
   anniversary: "2018-09-22"
   preferences: "{"preferences":["North Indian","Chinese"]}"
   createddate: "2023-01-10T09:30:45"
   modifieddate: "2023-01-10T09:30:45"
   ```

2. **Clean/Silver Layer:**
   - Data type conversions applied
   - Business rules implemented
   ```
   CUSTOMER_SK: 5001 (auto-generated)
   CUSTOMER_ID: "101"
   NAME: "Rahul Sharma"
   MOBILE: "9876543210"
   EMAIL: "rahul.sharma@gmail.com"
   LOGIN_BY_USING: "Google"
   GENDER: "Male"
   DOB: 1992-05-15 (converted to DATE type)
   ANNIVERSARY: 2018-09-22 (converted to DATE type)
   PREFERENCES: {"preferences":["North Indian","Chinese"]}
   CREATED_DT: 2023-01-10T09:30:45 (converted to TIMESTAMP_TZ)
   MODIFIED_DT: 2023-01-10T09:30:45 (converted to TIMESTAMP_TZ)
   ```

3. **Consumption/Gold Layer:**
   - SCD Type 2 applied with effective dates
   - Hash key generated from attributes
   ```
   CUSTOMER_HK: 7645382910 (hash key generated)
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

# 2. Customer Address Dimension: Insert Operation Flow

**Source Data (CSV):**
```
201,101,A-42,null,3rd,Sunshine Apartments,Near Metro Station,Koramangala,Bangalore,Karnataka,560034,12.9352|77.6245,Y,Home,2023-01-10T10:15:30,2023-01-10T10:15:30
```

**Flow Steps:**

1. **Stage/Bronze Layer:**
   - Raw data preserved as text
   ```
   addressid: "201"
   customerid: "101"
   flatno: "A-42"
   houseno: null
   floor: "3rd"
   building: "Sunshine Apartments"
   landmark: "Near Metro Station"
   locality: "Koramangala"
   city: "Bangalore"
   state: "Karnataka"
   pincode: "560034"
   coordinates: "12.9352|77.6245"
   primaryflag: "Y"
   addresstype: "Home"
   createddate: "2023-01-10T10:15:30"
   modifieddate: "2023-01-10T10:15:30"
   ```

2. **Clean/Silver Layer:**
   - Data types converted appropriately
   ```
   CUSTOMER_ADDRESS_SK: 3001 (auto-generated)
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
   CREATED_DATE: 2023-01-10T10:15:30 (converted to TIMESTAMP_TZ)
   MODIFIED_DATE: 2023-01-10T10:15:30 (converted to TIMESTAMP_TZ)
   ```

3. **Consumption/Gold Layer:**
   - SCD Type 2 with hash key and effective dates
   ```
   CUSTOMER_ADDRESS_HK: 8365219047 (hash key generated)
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

# 3. Date Dimension: Insert Operation Flow

The date dimension is populated differently, using a recursive CTE from the earliest order date to the current date.

**For a single date (2023-01-15):**

**Consumption Layer (Direct Insert):**
```
DATE_DIM_HK: 9273648501 (hash of date)
CALENDAR_DATE: 2023-01-15
YEAR: 2023
QUARTER: 1
MONTH: 1
WEEK: 3
DAY_OF_YEAR: 15
DAY_OF_WEEK: 7 (Sunday)
DAY_OF_THE_MONTH: 15
DAY_NAME: "Sunday"
```

Note: The date dimension generally doesn't go through the stage-clean-consumption flow as other entities do. It's typically loaded directly into the consumption layer using a script that calculates all date attributes.

# 4. Delivery Agent Dimension: Insert Operation Flow

**Source Data (CSV):**
```
301,Suresh Kumar,9876543210,Motorcycle,5,Active,Male,4.8,2023-01-11T08:00:00,2023-01-11T08:00:00
```

**Flow Steps:**

1. **Stage/Bronze Layer:**
   - Raw data as text
   ```
   deliveryagentid: "301"
   name: "Suresh Kumar"
   phone: "9876543210"
   vehicletype: "Motorcycle"
   locationid: "5"
   status: "Active"
   gender: "Male"
   rating: "4.8"
   createddate: "2023-01-11T08:00:00"
   modifieddate: "2023-01-11T08:00:00"
   ```

2. **Clean/Silver Layer:**
   - Data type conversions
   ```
   DELIVERY_AGENT_SK: 2001 (auto-generated)
   DELIVERY_AGENT_ID: 301
   NAME: "Suresh Kumar"
   PHONE: "9876543210"
   VEHICLE_TYPE: "Motorcycle"
   LOCATION_ID_FK: 5
   STATUS: "Active"
   GENDER: "Male"
   RATING: 4.8 (converted to number)
   CREATED_DT: 2023-01-11T08:00:00 (converted to TIMESTAMP_NTZ)
   MODIFIED_DT: 2023-01-11T08:00:00 (converted to TIMESTAMP_NTZ)
   ```

3. **Consumption/Gold Layer:**
   - SCD Type 2 implementation
   ```
   DELIVERY_AGENT_HK: 5731982064 (hash key generated)
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

# 5. Restaurant Location Dimension: Insert Operation Flow

**Source Data (CSV):**
```
5,Mumbai,Maharashtra,400001,Y,2023-01-05T10:00:00,2023-01-05T10:00:00
```

**Flow Steps:**

1. **Stage/Bronze Layer:**
   - Raw data preserved as text
   ```
   locationid: "5"
   city: "Mumbai"
   state: "Maharashtra"
   zipcode: "400001"
   activeflag: "Y"
   createdate: "2023-01-05T10:00:00"
   modifieddate: "2023-01-05T10:00:00"
   ```

2. **Clean/Silver Layer:**
   - Data type conversions plus business rule enrichment
   ```
   RESTAURANT_LOCATION_SK: 1001 (auto-generated)
   LOCATION_ID: 5
   CITY: "Mumbai"
   STATE: "Maharashtra"
   STATE_CODE: "MH" (derived)
   IS_UNION_TERRITORY: false (derived)
   CAPITAL_CITY_FLAG: true (derived)
   CITY_TIER: "Tier-1" (derived)
   ZIP_CODE: "400001"
   ACTIVE_FLAG: "Y"
   CREATED_TS: 2023-01-05T10:00:00 (converted to TIMESTAMP_TZ)
   MODIFIED_TS: 2023-01-05T10:00:00 (converted to TIMESTAMP_TZ)
   ```

3. **Consumption/Gold Layer:**
   - SCD Type 2 with effective dates
   ```
   RESTAURANT_LOCATION_HK: 3641897520 (hash key generated)
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

# 6. Restaurant Dimension: Insert Operation Flow

**Source Data (CSV):**
```
401,Spice Garden,North Indian|Chinese,800,9988776655,10:00-23:00,5,Y,Open,Bandra,Mumbai Premium Plaza Bandra West,19.0596,72.8295,2023-01-08T12:30:00,2023-01-08T12:30:00
```

**Flow Steps:**

1. **Stage/Bronze Layer:**
   - Raw text data preservation
   ```
   restaurantid: "401"
   name: "Spice Garden"
   cuisinetype: "North Indian|Chinese"
   pricing_for_2: "800"
   restaurant_phone: "9988776655"
   operatinghours: "10:00-23:00"
   locationid: "5"
   activeflag: "Y"
   openstatus: "Open"
   locality: "Bandra"
   restaurant_address: "Mumbai Premium Plaza Bandra West"
   latitude: "19.0596"
   longitude: "72.8295"
   createddate: "2023-01-08T12:30:00"
   modifieddate: "2023-01-08T12:30:00"
   ```

2. **Clean/Silver Layer:**
   - Data type conversions
   ```
   RESTAURANT_SK: 4001 (auto-generated)
   RESTAURANT_ID: 401
   NAME: "Spice Garden"
   CUISINE_TYPE: "North Indian|Chinese"
   PRICING_FOR_TWO: 800.00 (converted to DECIMAL)
   RESTAURANT_PHONE: "9988776655"
   OPERATING_HOURS: "10:00-23:00"
   LOCATION_ID_FK: 5
   ACTIVE_FLAG: "Y"
   OPEN_STATUS: "Open"
   LOCALITY: "Bandra"
   RESTAURANT_ADDRESS: "Mumbai Premium Plaza Bandra West"
   LATITUDE: 19.0596 (converted to NUMBER)
   LONGITUDE: 72.8295 (converted to NUMBER)
   CREATED_DT: 2023-01-08T12:30:00 (converted to TIMESTAMP_TZ)
   MODIFIED_DT: 2023-01-08T12:30:00 (converted to TIMESTAMP_TZ)
   ```

3. **Consumption/Gold Layer:**
   - SCD Type 2 implementation with hash key
   ```
   RESTAURANT_HK: 9157623048 (hash key generated)
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

# 7. Menu Dimension: Insert Operation Flow

**Source Data (CSV):**
```
501,401,Butter Chicken,Tender chicken in rich tomato gravy,350,Main Course,true,Non-Veg,2023-01-09T11:00:00,2023-01-09T11:00:00
```

**Flow Steps:**

1. **Stage/Bronze Layer:**
   - Raw text data
   ```
   menuid: "501"
   restaurantid: "401"
   itemname: "Butter Chicken"
   description: "Tender chicken in rich tomato gravy"
   price: "350"
   category: "Main Course"
   availability: "true"
   itemtype: "Non-Veg"
   createddate: "2023-01-09T11:00:00"
   modifieddate: "2023-01-09T11:00:00"
   ```

2. **Clean/Silver Layer:**
   - Data type conversions and validations
   ```
   MENU_SK: 6001 (auto-generated)
   MENU_ID: 501
   RESTAURANT_ID_FK: 401
   ITEM_NAME: "Butter Chicken"
   DESCRIPTION: "Tender chicken in rich tomato gravy"
   PRICE: 350.00 (converted to DECIMAL)
   CATEGORY: "Main Course"
   AVAILABILITY: true (converted to BOOLEAN)
   ITEM_TYPE: "Non-Veg"
   CREATED_DT: 2023-01-09T11:00:00 (converted to TIMESTAMP_NTZ)
   MODIFIED_DT: 2023-01-09T11:00:00 (converted to TIMESTAMP_NTZ)
   ```

3. **Consumption/Gold Layer:**
   - SCD Type 2 with hash key
   ```
   MENU_DIM_HK: 6258473910 (hash key generated)
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

# 8. Order Item Fact: Insert Operation Flow

For the fact table, we first need prerequisite data in the transactional tables:

**Order Source Data:**
```
1001,101,401,2023-01-15T19:30:00,700,Completed,UPI,2023-01-15T19:30:00,2023-01-15T20:30:00
```

**Order Item Source Data:**
```
2001,1001,501,2,350,700,2023-01-15T19:30:00,2023-01-15T19:30:00
```

**Delivery Source Data:**
```
3001,1001,301,Delivered,30 minutes,201,2023-01-15T20:15:00,2023-01-15T19:30:00,2023-01-15T20:15:00
```

**Flow Steps:**

1. **Stage/Bronze to Clean/Silver Layers** for transactional tables:
   - Orders, order items, and delivery records are processed through their respective ETL flows

2. **Fact Table Population:**
   - Join all dimension tables to create the fact record
   - Apply surrogate keys from dimensions

   ```
   ORDER_ITEM_FACT_SK: 8001 (auto-generated)
   ORDER_ITEM_ID: 2001
   ORDER_ID: 1001
   CUSTOMER_DIM_KEY: 7645382910 (hash key from customer dimension)
   CUSTOMER_ADDRESS_DIM_KEY: 8365219047 (hash key from address dimension)
   RESTAURANT_DIM_KEY: 9157623048 (hash key from restaurant dimension)
   RESTAURANT_LOCATION_DIM_KEY: 3641897520 (hash key from location dimension)
   MENU_DIM_KEY: 6258473910 (hash key from menu dimension)
   DELIVERY_AGENT_DIM_KEY: 5731982064 (hash key from delivery agent dimension)
   ORDER_DATE_DIM_KEY: 9273648501 (hash key from date dimension for Jan 15, 2023)
   QUANTITY: 2
   PRICE: 350.00
   SUBTOTAL: 700.00
   DELIVERY_STATUS: "Delivered"
   ESTIMATED_TIME: "30 minutes"
   ```