import mysql.connector
import pandas as pd

# ------------------ CONNECT TO MYSQL ------------------
conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="5002",
    database="hospital_queue"
)

# ------------------ LOAD DATA ------------------
query = "SELECT * FROM dataset"
df = pd.read_sql(query, conn)

print("\n--- DATA LOADED ---")
print(df.head())

# ------------------ FILTER ATTENDED PATIENTS ------------------
df = df[df['status'].str.lower() == 'attended']

# ------------------ CONVERT COLUMNS ------------------
df['waiting_time'] = pd.to_numeric(df['waiting_time'], errors='coerce')

df['appointment_time'] = pd.to_datetime(df['appointment_time'], errors='coerce')
df['check_in_time'] = pd.to_datetime(df['check_in_time'], errors='coerce')

df = df.dropna(subset=['appointment_time', 'check_in_time', 'waiting_time'])

# ------------------ CREATE PRIORITY ------------------
# Higher value = higher priority
def assign_priority(age):
    if age >= 60:
        return 3   # High priority (senior citizens)
    elif age >= 40:
        return 2   # Medium priority
    else:
        return 1   # Low priority

df['priority'] = df['age'].apply(assign_priority)

# ------------------ SORT BASED ON REAL QUEUE LOGIC ------------------
df = df.sort_values(by=['doctor_id', 'appointment_time', 'priority'], ascending=[True, True, False])

# ------------------ QUEUE SIMULATION ------------------
optimized_waiting = []
current_time_by_doctor = {}

for index, row in df.iterrows():
    doctor = row['doctor_id']
    
    # Initialize doctor availability time
    if doctor not in current_time_by_doctor:
        current_time_by_doctor[doctor] = row['check_in_time']
    
    current_time = current_time_by_doctor[doctor]
    
    # Patient can only start after arrival
    start_time = max(current_time, row['check_in_time'])
    
    # Assume each appointment takes 15 minutes
    end_time = start_time + pd.Timedelta(minutes=15)
    
    # Calculate waiting time
    waiting = (start_time - row['check_in_time']).total_seconds() / 60
    optimized_waiting.append(waiting)
    
    # Update doctor's next available time
    current_time_by_doctor[doctor] = end_time

# ------------------ SAVE RESULT ------------------
df['optimized_waiting_time'] = optimized_waiting

print("\n--- OPTIMIZED RESULT ---")
print(df[['patient_id', 'waiting_time', 'optimized_waiting_time', 'priority']].head())

# ------------------ EXPORT ------------------
df.to_csv("optimized_output_advanced.csv", index=False)

print("\nAdvanced optimization file saved as optimized_output_advanced.csv")