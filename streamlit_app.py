import streamlit as st
import pandas as pd
import sqlite3
from datetime import datetime, timedelta
from io import BytesIO
import os

# --- CONFIGURATION & DATABASE SETUP ---

st.set_page_config(page_title="Sunday School Attendance", page_icon="🙏", layout="wide")
# Set the database path inside a 'data' folder
DB_NAME = "data/sunday_school.db"

# Database initialization
def init_db():
    """Initializes the SQLite database with required tables and default session dates, 
    and ensures the 'students' table has the 'order_index' and 'classes' table has 'teacher_name' columns."""
    
    # 1. Ensure the persistent data directory exists
    data_dir = os.path.dirname(DB_NAME)
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)
        
    conn = sqlite3.connect(DB_NAME)
    # Ensure foreign keys are enforced (needed for ON DELETE CASCADE to work)
    conn.execute("PRAGMA foreign_keys = ON")
    
    # 1. Classes Table
    conn.execute('''
        CREATE TABLE IF NOT EXISTS classes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE NOT NULL,
            teacher_name TEXT 
        )
    ''')
    
    # ⚠️ FIX for existing databases: Ensure 'teacher_name' column exists
    try:
        conn.execute("SELECT teacher_name FROM classes LIMIT 1")
    except sqlite3.OperationalError:
        conn.execute("ALTER TABLE classes ADD COLUMN teacher_name TEXT")
        
    # 2. Students Table
    conn.execute('''
        CREATE TABLE IF NOT EXISTS students (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            class_id INTEGER,
            order_index INTEGER DEFAULT 0,
            FOREIGN KEY (class_id) REFERENCES classes (id) ON DELETE RESTRICT
        )
    ''')
    
    # ⚠️ FIX for existing databases: Ensure 'order_index' column exists
    try:
        # Attempt to read the column; if it fails, it means the column is missing
        conn.execute("SELECT order_index FROM students LIMIT 1")
    except sqlite3.OperationalError:
        # Column missing, add it with ALTER TABLE
        conn.execute("ALTER TABLE students ADD COLUMN order_index INTEGER DEFAULT 0")
        
    # 3. Attendance Table
    conn.execute('''
        CREATE TABLE IF NOT EXISTS attendance (
            date TEXT NOT NULL,
            student_id INTEGER NOT NULL,
            status TEXT NOT NULL, 
            PRIMARY KEY (date, student_id),
            FOREIGN KEY (student_id) REFERENCES students (id) ON DELETE CASCADE
        )
    ''')
    
    # 4. Sessions Table
    conn.execute('''
        CREATE TABLE IF NOT EXISTS sessions (
            key TEXT PRIMARY KEY,
            date_value TEXT
        )
    ''')
    
    # Initialize session dates if not present
    current_year = datetime.now().year
    default_start = f"{current_year}-01-01"
    default_end = f"{current_year}-12-31"

    if conn.execute("SELECT COUNT(*) FROM sessions WHERE key='start_date'").fetchone()[0] == 0:
        conn.execute("INSERT OR IGNORE INTO sessions (key, date_value) VALUES (?, ?)", ('start_date', default_start))
    if conn.execute("SELECT COUNT(*) FROM sessions WHERE key='end_date'").fetchone()[0] == 0:
        conn.execute("INSERT OR IGNORE INTO sessions (key, date_value) VALUES (?, ?)", ('end_date', default_end))

    conn.commit()
    conn.close()

# Ensure DB is initialized before continuing
init_db()

# --- NEW CLASS DELETION FUNCTION ---

def delete_class(class_id, class_name):
    """Deletes a class, but prevents deletion if students are assigned (ON DELETE RESTRICT)."""
    conn = sqlite3.connect(DB_NAME)
    conn.execute("PRAGMA foreign_keys = ON")
    
    try:
        # Check if any students are in this class
        student_count = conn.execute("SELECT COUNT(*) FROM students WHERE class_id = ?", (class_id,)).fetchone()[0]
        
        if student_count > 0:
            st.error(f"Cannot delete class '{class_name}': {student_count} student(s) are still assigned to it. Please move or delete students first.")
            return False
            
        conn.execute("DELETE FROM classes WHERE id = ?", (class_id,))
        conn.commit()
        st.success(f"Class '{class_name}' deleted successfully!")
        st.cache_data.clear()
        st.rerun()
        return True
    except Exception as e:
        st.error(f"Error deleting class: {e}")
        return False
    finally:
        conn.close()

# --- UTILITY FUNCTIONS ---

@st.cache_data
def load_data():
    """Loads all essential data frames and session dates from the DB."""
    conn = sqlite3.connect(DB_NAME)
    
    # SELECT * is used to automatically pick up the new teacher_name column
    df_classes = pd.read_sql("SELECT * FROM classes ORDER BY name", conn)
    
    # Include c.teacher_name in the student query
    df_students = pd.read_sql(
        "SELECT s.id, s.name, c.name AS class_name, c.teacher_name, s.class_id, s.order_index FROM students s LEFT JOIN classes c ON s.class_id = c.id ORDER BY c.name, s.order_index, s.name", 
        conn
    )
    
    # Ensure 'date' is parsed as datetime when reading from DB
    df_attendance = pd.read_sql("SELECT * FROM attendance", conn, parse_dates=['date']) 
    
    # Load session dates
    session_data = pd.read_sql("SELECT key, date_value FROM sessions", conn).set_index('key')['date_value'].to_dict()
    
    conn.close()
    
    current_year = datetime.now().year
    start_date = pd.to_datetime(session_data.get('start_date', f'{current_year}-01-01')).date()
    end_date = pd.to_datetime(session_data.get('end_date', f'{current_year}-12-31')).date()
    
    return df_classes, df_students, df_attendance, start_date, end_date

def get_all_sundays(start_date, end_date):
    """Generates a list of all Sundays between the start and end dates."""
    sundays = []
    # Find the next Sunday on or after the start date (Sunday is 6 in weekday())
    current_date = start_date + timedelta(days=(6 - start_date.weekday() + 7) % 7)
    
    while current_date <= end_date:
        sundays.append(current_date)
        current_date += timedelta(days=7)
    return sundays

def to_excel(df):
    """Converts a DataFrame to an Excel file stored in a BytesIO buffer."""
    output = BytesIO()
    try:
        import xlsxwriter
        
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            df.to_excel(writer, index=False, sheet_name='Attendance Report') 
        processed_data = output.getvalue()
        return processed_data
    except ImportError:
        st.error("Cannot generate Excel file: 'xlsxwriter' library is required. Please run: pip install xlsxwriter")
        return None
    except Exception as e:
        st.error(f"An error occurred while generating Excel: {e}")
        return None

def delete_student(student_id):
    """Deletes a student and their associated attendance records (via CASCADE)."""
    conn = sqlite3.connect(DB_NAME)
    # Ensure foreign keys ON for this connection too
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("DELETE FROM students WHERE id = ?", (student_id,))
    conn.commit()
    conn.close()
    st.cache_data.clear() 
    st.rerun()

def promote_student(student_id, new_class_id, new_class_name, student_name):
    """Moves a student to a new class and resets their order_index."""
    conn = sqlite3.connect(DB_NAME)
    try:
        # 1. Determine the new order_index (last in the new class)
        max_order_query = conn.execute(
            "SELECT COALESCE(MAX(order_index), 0) FROM students WHERE class_id = ?", 
            (new_class_id,)
        ).fetchone()[0]
        new_order = int(max_order_query) + 1
        
        # 2. Update the student's class_id and order_index
        conn.execute(
            "UPDATE students SET class_id = ?, order_index = ? WHERE id = ?", 
            (new_class_id, new_order, student_id)
        )
        conn.commit()
        st.success(f"✅ Student **{student_name}** successfully moved to **{new_class_name}**.")
        st.info("Their previous attendance records have been preserved.")
        st.cache_data.clear()
        st.rerun()
    except Exception as e:
        st.error(f"Error promoting student: {e}")
    finally:
        conn.close()


def move_student_order(student_id, direction, df_students_current_class):
    """Moves a student up or down in the current class's list by swapping order_index."""
    
    student_list_ids = df_students_current_class['id'].tolist()
    
    try:
        current_index = student_list_ids.index(student_id)
    except ValueError:
        return 

    if direction == 'up':
        target_index = max(0, current_index - 1)
    elif direction == 'down':
        target_index = min(len(df_students_current_class) - 1, current_index + 1)
    else:
        return

    if current_index != target_index:
        
        id_to_move = student_list_ids[current_index]
        id_to_swap = student_list_ids[target_index]
        
        order_to_move = df_students_current_class.loc[df_students_current_class['id'] == id_to_move, 'order_index'].iloc[0]
        order_to_swap = df_students_current_class.loc[df_students_current_class['id'] == id_to_swap, 'order_index'].iloc[0]
        
        conn = sqlite3.connect(DB_NAME)
        conn.execute("UPDATE students SET order_index = ? WHERE id = ?", (order_to_swap, id_to_move))
        conn.execute("UPDATE students SET order_index = ? WHERE id = ?", (order_to_move, id_to_swap))
        conn.commit()
        conn.close()
        
        st.cache_data.clear() 
        st.rerun()


# Load all data initially
df_classes, df_students, df_attendance, session_start_date, session_end_date = load_data()
all_sundays = get_all_sundays(session_start_date, session_end_date)


# --- UI: HEADER ---

st.title("🙏 Sunday School Attendance Log")
st.markdown(f"**Current Session:** {session_start_date.strftime('%d %b %Y')} to {session_end_date.strftime('%d %b %Y')}")
st.markdown("")


# --------------------------------------------------------------------------------------------------
## 🛠️ School Setup: Session Dates & Classes
# --------------------------------------------------------------------------------------------------

with st.expander("🛠️ School Setup: Session Dates & Classes", expanded=False):
    
    st.subheader("1. Set School Session Dates")
    with st.form("session_form"):
        col_s, col_e = st.columns(2)
        new_start_date = col_s.date_input("Session Start Date", session_start_date)
        new_end_date = col_e.date_input("Session End Date", session_end_date)
        
        submitted_session = st.form_submit_button("Update Session Dates", type="primary")
        
        if submitted_session:
            if new_start_date >= new_end_date:
                st.error("Start date must be before end date.")
            else:
                conn = sqlite3.connect(DB_NAME)
                conn.execute("INSERT OR REPLACE INTO sessions (key, date_value) VALUES (?, ?)", ('start_date', new_start_date.isoformat()))
                conn.execute("INSERT OR REPLACE INTO sessions (key, date_value) VALUES (?, ?)", ('end_date', new_end_date.isoformat()))
                conn.commit()
                conn.close()
                st.success(f"Session updated from {new_start_date} to {new_end_date}.")
                st.cache_data.clear() 
                st.rerun() 

    st.markdown("---")
    st.subheader("2. Add/Delete Classes")
    
    col_add, col_del = st.columns([1, 1])
    
    # --- ADD CLASS UI ---
    with col_add:
        st.markdown("##### Add New Class")
        # Added input for teacher name
        new_class_name = st.text_input("New Class Name (e.g., Beginner, Primary)", key="new_class_name_input")
        new_class_teacher = st.text_input("Teacher Name for New Class", key="new_class_teacher_input")
        
        if st.button("Add Class", key="add_class_btn"):
            if new_class_name:
                try:
                    conn = sqlite3.connect(DB_NAME)
                    # INSERT includes the new teacher_name
                    conn.execute("INSERT INTO classes (name, teacher_name) VALUES (?, ?)", (new_class_name, new_class_teacher.strip()))
                    conn.commit()
                    conn.close()
                    st.success(f"Class '{new_class_name}' added! Teacher: {new_class_teacher.strip() if new_class_teacher else 'N/A'}")
                    st.cache_data.clear()
                    st.rerun()
                except sqlite3.IntegrityError:
                    st.warning(f"Class '{new_class_name}' already exists.")
            else:
                st.error("Please enter a class name.")
    
    # --- DELETE CLASS UI ---
    with col_del:
        st.markdown("##### Delete Class")
        class_options_del = ['-- Select Class to Delete --'] + df_classes['name'].tolist()
        class_map_del = dict(zip(df_classes['name'], df_classes['id']))

        class_to_delete_name = st.selectbox(
            "Select Class to Delete", 
            options=class_options_del, 
            key="delete_class_select"
        )
        
        if class_to_delete_name != '-- Select Class to Delete --':
            class_to_delete_id = class_map_del[class_to_delete_name]
            if st.button(f"🗑️ Delete: {class_to_delete_name}", key="confirm_delete_class"):
                # Call the new delete function
                delete_class(class_to_delete_id, class_to_delete_name)

    st.markdown("---")
    
    # --- 3. ASSIGN/EDIT TEACHER ---
    st.subheader("3. Assign/Edit Class Teacher")
    
    if not df_classes.empty:
        class_names = df_classes['name'].tolist()
        class_map = dict(zip(df_classes['name'], df_classes['id']))
        
        selected_class_edit = st.selectbox(
            "Select Class to Edit Teacher", 
            options=class_names, 
            key="edit_class_select"
        )
        
        # Get current teacher name
        current_teacher = df_classes[df_classes['name'] == selected_class_edit]['teacher_name'].iloc[0] or ""

        with st.form("edit_teacher_form"):
            new_teacher_name = st.text_input(
                f"New Teacher Name for **{selected_class_edit}**", 
                value=current_teacher, 
                key="edit_teacher_input"
            )
            
            if st.form_submit_button("Update Teacher Name", type="secondary"):
                class_id_to_edit = class_map[selected_class_edit]
                conn = sqlite3.connect(DB_NAME)
                conn.execute(
                    "UPDATE classes SET teacher_name = ? WHERE id = ?", 
                    (new_teacher_name.strip(), class_id_to_edit)
                )
                conn.commit()
                conn.close()
                st.success(f"Teacher for {selected_class_edit} updated to: {new_teacher_name.strip()}")
                st.cache_data.clear()
                st.rerun()
    else:
        st.info("No classes available to assign teachers.")
        
    st.markdown("---")
    
    # --- Current Classes Overview (NEW EXPANDER) ---
    with st.expander("Current Classes Overview", expanded=False):
        if not df_classes.empty:
            # Display Class Name and Teacher Name
            df_display_classes = df_classes.rename(columns={'name': 'Class Name', 'teacher_name': 'Teacher'})
            # Use .fillna('') to display blank instead of 'None' for classes with no assigned teacher
            df_display_classes['Teacher'] = df_display_classes['Teacher'].fillna('N/A') 
            st.dataframe(df_display_classes[['Class Name', 'Teacher']], hide_index=True, use_container_width=True)
        else:
            st.info("No classes added yet.")


# --------------------------------------------------------------------------------------------------
## 👥 Student Management
# --------------------------------------------------------------------------------------------------

# Set expanded=False to collapse by default as requested
with st.expander("👥 Student Management", expanded=False):
    
    if df_classes.empty:
        st.warning("Please add at least one class in the 'School Setup' section first.")
    else:
        # --- 1. Add New Student ---
        st.subheader("1. Add New Student")
        with st.form("student_form"):
            col_s_name, col_s_class = st.columns(2)
            student_name = col_s_name.text_input("Student Name")
            
            # The class_options dictionary correctly maps name to ID
            class_options = dict(zip(df_classes['name'], df_classes['id']))
            selected_class = col_s_class.selectbox("Assign to Class", options=list(class_options.keys()), key="new_student_class_select")
            
            submitted_student = st.form_submit_button("Add Student", type="primary")
            
            if submitted_student:
                try:
                    if student_name and selected_class:
                        class_id = class_options[selected_class]
                        conn = sqlite3.connect(DB_NAME)
                        
                        # When adding, set the order_index to be the max existing order + 1 for this class
                        # Need to reload students list for max_order if cache is not cleared immediately after add/delete
                        max_order_query = conn.execute(
                            "SELECT COALESCE(MAX(order_index), 0) FROM students WHERE class_id = ?", 
                            (class_id,)
                        ).fetchone()[0]
                        new_order = int(max_order_query) + 1
                        
                        conn.execute("INSERT INTO students (name, class_id, order_index) VALUES (?, ?, ?)", (student_name, class_id, new_order))
                        conn.commit()
                        conn.close()
                        st.success(f"Student '{student_name}' added to {selected_class}!")
                        st.cache_data.clear()
                        st.rerun()
                    else:
                        st.error("Please enter a name and select a class.")
                except sqlite3.IntegrityError as e:
                    # This catches the specific Foreign Key error
                    st.error(f"Error adding student: Integrity Error. Class ID may be corrupted. Please delete and re-add the class in the School Setup section. Details: {e}")
                except Exception as e:
                    st.error(f"An unexpected error occurred: {e}")


        st.markdown("")
        
        # --- 2. Reorder Students UI ---
        st.subheader("2. Reorder Students (Move Up/Down)")
        
        if not df_students.empty:
            
            # Filter out students without a valid class_name (where the LEFT JOIN failed)
            valid_classes_in_students = df_students['class_name'].dropna().unique().tolist()
            
            if not valid_classes_in_students:
                st.info("No valid students found. Check your database migration or add a new student.")
            else:
                class_reorder_options = valid_classes_in_students
                selected_class_reorder = st.selectbox(
                    "Filter Class to Reorder", 
                    options=class_reorder_options,
                    key="reorder_class_select"
                )
                
                df_reorder_students = df_students[df_students['class_name'] == selected_class_reorder].sort_values(by=['order_index', 'name']).reset_index(drop=True)
                
                # --- NEW EXPANDER FOR THE STUDENT LIST ---
                with st.expander(f"Current Order in {selected_class_reorder} ({len(df_reorder_students)} students)", expanded=False):
                    
                    if not df_reorder_students.empty:
                        st.info("Use the buttons to change the order in which students appear for attendance marking.")
                        
                        for index, row in df_reorder_students.iterrows():
                            student_id = row['id']
                            student_name = row['name']
                            
                            col_idx, col_name, col_up, col_down = st.columns([0.5, 4, 1, 1])
                            
                            col_idx.write(f"**{index + 1}.**")
                            col_name.write(f"**{student_name}**")
                            
                            if index > 0:
                                if col_up.button("⬆️ Up", key=f"move_up_{student_id}", help="Move student up in the list."):
                                    move_student_order(student_id, 'up', df_reorder_students)
                            else:
                                col_up.empty()
                                
                            if index < len(df_reorder_students) - 1:
                                if col_down.button("⬇️ Down", key=f"move_down_{student_id}", help="Move student down in the list."):
                                    move_student_order(student_id, 'down', df_reorder_students)
                            else:
                                col_down.empty()
                    else:
                        st.info(f"No students found in the '{selected_class_reorder}' class to reorder.")

        else:
            st.info("No students added yet.")

        st.markdown("---")
        
        # --- 3. Delete Student ---
        st.subheader("3. Delete Student")
        
        if not df_students.empty:
            
            # Filter by Class is kept outside the inner expander for usability
            class_options_del_student = ['-- All Classes (Show All Students) --'] + df_classes['name'].tolist()
            selected_class_filter = st.selectbox(
                "Filter Student List by Class",
                options=class_options_del_student,
                key="delete_student_class_filter"
            )

            # Filter students DataFrame (logic remains the same)
            if selected_class_filter != '-- All Classes (Show All Students) --':
                class_id_filter_row = df_classes[df_classes['name'] == selected_class_filter]
                if not class_id_filter_row.empty:
                    class_id_filter = class_id_filter_row['id'].iloc[0]
                    df_students_filtered = df_students[df_students['class_id'] == class_id_filter]
                else:
                    df_students_filtered = pd.DataFrame() 
            else:
                df_students_filtered = df_students.copy()
            
            # --- NEW EXPANDER FOR DELETION SELECTION ---
            if df_students_filtered.empty:
                 st.info(f"No students found in the '{selected_class_filter}' list.")
            else:
                student_options = df_students_filtered.apply(lambda row: f"{row['name']} ({row['class_name'] if pd.notna(row['class_name']) else 'No Class'})", axis=1).tolist()
                student_id_map = dict(zip(student_options, df_students_filtered['id']))
                
                with st.expander(f"Select Student to Delete ({len(student_options)} found in filter)", expanded=False):
                    
                    selected_student_key = st.selectbox(
                        "Select Student to Delete", 
                        options=['-- Select Student --'] + student_options,
                        key="student_delete_select"
                    )

                    if selected_student_key != '-- Select Student --':
                        student_to_delete_id = student_id_map[selected_student_key]
                        
                        if st.button(f"🗑️ Permanently Delete: {selected_student_key}", help="This action is irreversible and will delete all attendance records for this student.", type="secondary"):
                            delete_student(student_to_delete_id)
        else:
            st.info("No students to delete.")
            
        st.markdown("---")
        
        # --- 4. Promote/Move Student ---
        st.subheader("4. Promote/Move Student")
        
        # --- NEW EXPANDER FOR PROMOTION ---
        with st.expander("Move Student to Another Class (Preserves History)", expanded=False):
            st.info("Use this to change a student's class (e.g., for a new school session) while keeping their old attendance records.")
            
            if not df_students.empty:
                
                student_options_move = df_students.apply(lambda row: f"{row['name']} (Current: {row['class_name'] if pd.notna(row['class_name']) else 'No Class'})", axis=1).tolist()
                student_id_map_move = dict(zip(student_options_move, df_students['id']))
                
                col_move_stu, col_move_class = st.columns(2)
                
                selected_student_key_move = col_move_stu.selectbox(
                    "Select Student to Move", 
                    options=['-- Select Student --'] + student_options_move,
                    key="student_move_select"
                )
                
                # Filter the list of classes to exclude the current class (if one is selected)
                class_options_move = ['-- Select New Class --'] + df_classes['name'].tolist()
                class_id_map_move = dict(zip(df_classes['name'], df_classes['id']))
                
                selected_class_name_move = col_move_class.selectbox(
                    "Select New Class", 
                    options=class_options_move,
                    key="class_move_select"
                )
                
                if selected_student_key_move != '-- Select Student --' and selected_class_name_move != '-- Select New Class --':
                    
                    student_to_move_id = student_id_map_move[selected_student_key_move]
                    new_class_to_move_id = class_id_map_move[selected_class_name_move]
                    
                    # Get student details for confirmation
                    student_name = df_students[df_students['id'] == student_to_move_id]['name'].iloc[0]
                    current_class_id = df_students[df_students['id'] == student_to_move_id]['class_id'].iloc[0]
                    
                    if current_class_id == new_class_to_move_id:
                        st.warning("The selected student is already in this class.")
                    elif st.button(f"➡️ Move {student_name} to {selected_class_name_move} (Preserve History)", type="primary"):
                        promote_student(student_to_move_id, new_class_to_move_id, selected_class_name_move, student_name)
                        
            else:
                st.info("No students to promote or move.")


# --------------------------------------------------------------------------------------------------
## 📝 Attendance Entry
# --------------------------------------------------------------------------------------------------

st.header("📝 Attendance Entry")
st.markdown("")

if df_students.empty:
    st.warning("Please add students to classes before taking attendance.")
else:
    col_date, col_class = st.columns(2)
    
    # Logic: Only show Sundays up to (and including) today for attendance marking/back entry
    valid_sundays = [d for d in all_sundays if d <= datetime.today().date()]
    
    if not valid_sundays:
        st.info("No Sundays available in the current session yet, or today is not a Sunday.")
    else:
        
        # Default to the most recent Sunday (which allows back entry, or marks today if today is Sunday)
        default_index = len(valid_sundays) - 1
        
        selected_date = col_date.selectbox(
            "Select Sunday Date",
            options=valid_sundays,
            format_func=lambda d: d.strftime('%d %b %Y'),
            index=default_index,
            key="att_date_select"
        )
        
        class_options_for_select = dict(zip(df_classes['name'], df_classes['id']))
        selected_class_name = col_class.selectbox("Select Class to Mark", options=list(class_options_for_select.keys()), key="att_class_select")
        selected_class_id = class_options_for_select.get(selected_class_name)
        
        st.markdown("")

        class_students = df_students[df_students['class_id'] == selected_class_id].sort_values(by=['order_index', 'name']).reset_index(drop=True)
        
        if class_students.empty:
            st.info(f"No students found in the '{selected_class_name}' class.")
        else:
            
            existing_attendance = df_attendance[
                (df_attendance['date'].dt.date == selected_date) & 
                (df_attendance['student_id'].isin(class_students['id']))
            ]
            
            initial_no_class = existing_attendance['status'].eq('N/C').any() if not existing_attendance.empty else False

            st.subheader(f"Mark Attendance for {selected_class_name} on {selected_date.strftime('%d %b %Y')}")
            
            # --- Attendance Marking is wrapped in an expander ---
            with st.expander("Mark Students Present/Absent", expanded=True):
                
                no_class_check = st.checkbox(
                    "No Class/School Closed", 
                    value=initial_no_class, 
                    key="nc_check_state",
                    help="Check this if the school was closed or the class did not meet on this date."
                )
                
                attendance_status = {}
                
                # --- Attendance Form ---
                with st.form("attendance_form"):
                    st.markdown("##### Student List:")
                    
                    if not no_class_check:
                        for index, row in class_students.iterrows():
                            student_id = row['id']
                            student_name = row['name']
                            
                            current_record = existing_attendance[existing_attendance['student_id'] == student_id]
                            initial_status = current_record['status'].iloc[0] if not current_record.empty and current_record['status'].iloc[0] != 'N/C' else 'P'
                            
                            key = f"status_{student_id}"
                            
                            col_stu, col_status = st.columns([3, 2])
                            col_stu.markdown(f"**{index+1}. {student_name}**")
                            
                            attendance_status[student_id] = col_status.radio(
                                "Status", 
                                options=['P', 'A'], 
                                index=0 if initial_status == 'P' else 1, 
                                key=key,
                                horizontal=True,
                                label_visibility="collapsed"
                            )
                    else:
                        st.info("The 'No Class' checkbox is checked. All students in this class will be marked 'N/C' upon saving.")

                    st.markdown("")
                    submitted_attendance = st.form_submit_button("SAVE ATTENDANCE", type="primary")

                    if submitted_attendance:
                        conn = sqlite3.connect(DB_NAME)
                        date_str = selected_date.isoformat()
                        
                        if no_class_check:
                            for student_id in class_students['id']:
                                conn.execute("INSERT OR REPLACE INTO attendance (date, student_id, status) VALUES (?, ?, ?)", 
                                             (date_str, student_id, 'N/C'))
                            st.success(f"Attendance for {selected_class_name} on {date_str} saved as **NO CLASS**.")
                        else:
                            for student_id, status in attendance_status.items():
                                conn.execute("INSERT OR REPLACE INTO attendance (date, student_id, status) VALUES (?, ?, ?)", 
                                             (date_str, student_id, status))
                            st.success(f"Attendance for {selected_class_name} on {date_str} successfully saved.")

                        conn.commit()
                        conn.close()
                        st.cache_data.clear()
                        st.rerun()


# --------------------------------------------------------------------------------------------------
## 📊 Attendance Reports
# --------------------------------------------------------------------------------------------------

st.header("📊 Attendance Reports")
st.markdown("")

if df_attendance.empty or df_students.empty:
    st.info("No attendance data or students available to generate reports.")
else:
    st.subheader("Generate School Report (All Students)")
    
    col_r_start, col_r_end = st.columns(2)
    
    # Report generation uses ALL Sundays in the session range
    default_start_index = 0
    default_end_index = len(all_sundays) - 1 if len(all_sundays) > 0 else 0

    report_start_date = col_r_start.selectbox(
        "From Date (Sunday)", 
        options=all_sundays, 
        format_func=lambda d: d.strftime('%d %b %Y'), 
        index=default_start_index, 
        key="report_start"
    )
    report_end_date = col_r_end.selectbox(
        "To Date (Sunday)", 
        options=all_sundays, 
        format_func=lambda d: d.strftime('%d %b %Y'), 
        index=default_end_index, 
        key="report_end"
    )
    
    if report_start_date > report_end_date:
        st.error("The 'From Date' cannot be after the 'To Date'. Please adjust the range.")
    else:
        
        if 'df_final_report' not in st.session_state:
            st.session_state.df_final_report = pd.DataFrame()

        if st.button("Generate Report", key="generate_report_btn", type="primary"):
            
            all_sundays_in_range = [d for d in all_sundays if report_start_date <= d <= report_end_date]
            
            df_filtered_att = df_attendance[
                (df_attendance['date'].dt.date >= report_start_date) & 
                (df_attendance['date'].dt.date <= report_end_date)
            ].copy()
            
            # Include teacher_name in the report base DataFrame
            df_report = df_students[['id', 'name', 'class_name', 'teacher_name', 'class_id']].copy()
            df_report.rename(
                columns={
                    'id': 'student_id', 
                    'name': 'Student Name', 
                    'class_name': 'Class', 
                    'teacher_name': 'Teacher' # Renamed for display
                }, 
                inplace=True
            )
            
            final_report_data = []
            
            for _, student_row in df_report.iterrows():
                student_id = student_row['student_id']
                student_class_id = student_row['class_id']
                
                student_att = df_filtered_att[df_filtered_att['student_id'] == student_id]
                
                total_classes_for_student = 0
                date_status = {}
                
                for sunday in all_sundays_in_range:
                    sunday_key = sunday.isoformat()
                    
                    is_no_class = df_filtered_att[
                        (df_filtered_att['date'].dt.date == sunday) &
                        (df_filtered_att['student_id'].isin(df_students[df_students['class_id'] == student_class_id]['id'])) &
                        (df_filtered_att['status'] == 'N/C')
                    ].shape[0] > 0
                    
                    if is_no_class:
                        status = 'N/C'
                    else:
                        status_record = student_att[student_att['date'].dt.date == sunday] 
                        if not status_record.empty:
                            status = status_record['status'].iloc[0]
                        else:
                            status = 'A (M)' 
                        
                        if status in ['P', 'A', 'A (M)']:
                            total_classes_for_student += 1 

                    date_status[sunday_key] = status

                attended = list(date_status.values()).count('P')
                percentage = (attended / total_classes_for_student * 100) if total_classes_for_student > 0 else 0
                
                final_row = {
                    'Class': student_row['Class'],
                    'Teacher': student_row['Teacher'], # ADDED Teacher
                    'Student Name': student_row['Student Name'],
                    'Total Classes': total_classes_for_student,
                    'Attended': attended,
                    'Attendance %': f"{percentage:.1f}%"
                }
                final_row.update(date_status)
                final_report_data.append(final_row)

            df_final_report = pd.DataFrame(final_report_data)
            
            date_cols_keys = [d.isoformat() for d in all_sundays_in_range]
            
            # Include 'Teacher' in the display order
            display_cols_order = (
                ['Class', 'Teacher', 'Student Name'] + 
                date_cols_keys + 
                ['Total Classes', 'Attended', 'Attendance %']
            )
            
            st.session_state.df_final_report = df_final_report[display_cols_order]
            st.session_state.report_date_range = f"{report_start_date.strftime('%Y%m%d')}_{report_end_date.strftime('%Y%m%d')}"

            st.success(f"Report Generated for {report_start_date.strftime('%d %b %Y')} - {report_end_date.strftime('%d %b %Y')}")
        
        # --- 5. Display and Download Report ---
        if not st.session_state.df_final_report.empty:
            
            # --- Report Display is wrapped in an expander ---
            with st.expander("View Detailed Report", expanded=True):
            
                df_display = st.session_state.df_final_report.copy()
                df_excel_export = st.session_state.df_final_report.copy()
                
                # Report date columns start after the first three columns: Class, Teacher, Student Name
                df_report_dates_iso = df_display.columns[3:-3] 
                
                column_rename_map = {}
                for date_key in df_report_dates_iso:
                    try:
                        date_obj = datetime.strptime(date_key, '%Y-%m-%d').date()
                        column_rename_map[date_key] = date_obj.strftime('%d %b').upper()
                    except ValueError:
                        column_rename_map[date_key] = date_key 

                df_display.rename(columns=column_rename_map, inplace=True)
                
                # Include 'Teacher' in the final display column names
                display_col_names = (
                    ['Class', 'Teacher', 'Student Name'] + 
                    list(column_rename_map.values()) +
                    ['Total Classes', 'Attended', 'Attendance %']
                )
                
                st.dataframe(df_display[display_col_names], hide_index=True, use_container_width=True)
                
                df_excel_export.rename(columns=column_rename_map, inplace=True)
                
                report_xlsx = to_excel(df_excel_export)
                
                if report_xlsx is not None:
                    download_filename = f"Sunday_School_Attendance_Report_{st.session_state.report_date_range}.xlsx"
                    
                    st.download_button(
                        label="📥 Download Report as Excel (.xlsx)",
                        data=report_xlsx,
                        file_name=download_filename,
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        key="download_excel_btn"
                    )

            # --- Summary Metrics ---
            
            st.markdown("")
            st.subheader("Summary Metrics")
            
            non_nc_att = df_attendance[df_attendance['status'] != 'N/C']
            
            class_days = non_nc_att[
                (non_nc_att['date'].dt.date >= report_start_date) & 
                (non_nc_att['date'].dt.date <= report_end_date)
            ]
            
            class_days = class_days.merge(df_students[['id', 'class_id']], left_on='student_id', right_on='id', how='left')
            total_school_days_held = class_days[['date', 'class_id']].drop_duplicates().shape[0]
            
            col_sum1, col_sum2, col_sum3 = st.columns(3)
            all_sundays_report = [d for d in all_sundays if report_start_date <= d <= report_end_date]
            
            col_sum1.metric("Total Sundays in Range", len(all_sundays_report))
            col_sum2.metric("Total Class Sessions Held", total_school_days_held, help="Total number of Sunday/Class combinations where a class was marked P or A in the selected range.")
            col_sum3.metric("Total Students Enrolled", len(df_students))
            
            st.markdown("")


# --- FOOTER ---
st.caption("Developed using Streamlit and SQLite for digital attendance logging.")