import psycopg2


def connect_db():
    """连接到 openGauss 数据库"""
    return psycopg2.connect(
        dbname="cse_u202112181",
        user="czr",
        password="cse_czr@123",
        host="127.0.0.1",
        port="15432"
    )


def initialize_or_update_student_info():
    """初始化或更新学生信息"""
    sno = input("Enter student number: ")
    sname = input("Enter student name: ")
    ssex = input("Enter student sex (M/F): ")
    sage = int(input("Enter student age: "))
    sdept = input("Enter department: ")
    scholarship = input("Has scholarship? (True/False): ")

    conn = connect_db()
    cur = conn.cursor()

    # 检查学号是否已存在
    cur.execute("SELECT * FROM Student WHERE Sno = %s", (sno,))
    if cur.fetchone():
        # 如果存在，执行更新操作
        cur.execute("""
            UPDATE Student
            SET Sname = %s, Ssex = %s, Sage = %s, Sdept = %s, Scholarship = %s
            WHERE Sno = %s
        """, (sname, ssex, sage, sdept, scholarship, sno))
        print("Student information updated successfully.")
    else:
        # 如果不存在，执行插入操作
        cur.execute("""
            INSERT INTO Student (Sno, Sname, Ssex, Sage, Sdept, Scholarship)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (sno, sname, ssex, sage, sdept, scholarship))
        print("New student information initialized successfully.")

    conn.commit()
    cur.close()
    conn.close()

def update_course_info():
    """更新课程信息：添加、修改、删除课程"""
    conn = connect_db()
    cur = conn.cursor()

    print("Choose an option:")
    print("1. Add a new course")
    print("2. Update an existing course")
    print("3. Delete a course")
    choice = input("Enter your choice (1, 2, or 3): ")

    if choice == '1':
        # 添加新课程
        cno = input("Enter course number: ")
        cname = input("Enter course name: ")
        ccredit = input("Enter course credits: ")
        cur.execute("INSERT INTO Course (Cno, Cname, Ccredit) VALUES (%s, %s, %s)",
                    (cno, cname, ccredit))
        print("New course added successfully.")

    elif choice == '2':
        # 更新现有课程
        cno = input("Enter course number to update: ")
        new_cname = input("Enter new course name: ")
        new_ccredit = input("Enter new course credits: ")
        cur.execute("UPDATE Course SET Cname = %s, Ccredit = %s WHERE Cno = %s",
                    (new_cname, new_ccredit, cno))
        print("Course updated successfully.")

    elif choice == '3':
        # 删除课程
        cno = input("Enter course number to delete: ")
        # 先检查该课程是否有学生选修
        cur.execute("SELECT * FROM SC WHERE Cno = %s", (cno,))
        if cur.fetchone():
            print("Cannot delete course: it has enrollments.")
        else:
            cur.execute("DELETE FROM Course WHERE Cno = %s", (cno,))
            print("Course deleted successfully.")

    else:
        print("Invalid choice.")

    conn.commit()
    cur.close()
    conn.close()


def input_student_grades():
    """录入或更新学生成绩"""
    sno = input("Enter student number: ")
    cno = input("Enter course number: ")
    grade = int(input("Enter grade: "))
    conn = connect_db()
    cur = conn.cursor()

    # 检查学生是否已经选了这门课
    cur.execute("SELECT * FROM SC WHERE Sno = %s AND Cno = %s", (sno, cno))
    if cur.fetchone():
        # 如果记录存在，更新成绩
        cur.execute("UPDATE SC SET Grade = %s WHERE Sno = %s AND Cno = %s", (grade, sno, cno))
        print("Grade updated successfully.")
    else:
        # 如果记录不存在，插入新记录
        cur.execute("INSERT INTO SC (Sno, Cno, Grade) VALUES (%s, %s, %s)", (sno, cno, grade))
        print("New grade record inserted successfully.")

    conn.commit()
    cur.close()
    conn.close()


def query_student_grades():
    """查询特定学生的学业成绩"""
    sno = input("Enter student number: ")
    conn = connect_db()
    cur = conn.cursor()
    cur.execute("SELECT c.Cname, sc.Grade FROM SC sc JOIN Course c ON sc.Cno = c.Cno WHERE sc.Sno = %s", (sno,))
    grades = cur.fetchall()
    for grade in grades:
        print(f"Course: {grade[0]}, Grade: {grade[1]}")
    cur.close()
    conn.close()


def calculate_department_statistics():
    """Calculate statistics per department including average, max, min grades, excellence rate, and failure count."""
    conn = connect_db()
    cur = conn.cursor()

    # Query for average, maximum, and minimum grades per department
    cur.execute("""
        SELECT s.Sdept, AVG(sc.Grade) AS avg_grade, MAX(sc.Grade) AS max_grade, MIN(sc.Grade) AS min_grade
        FROM Student s
        JOIN SC sc ON s.Sno = sc.Sno
        GROUP BY s.Sdept
    """)
    results = cur.fetchall()
    for result in results:
        print(
            f"Department: {result[0]}, Average Grade: {result[1]:.2f}, Max Grade: {result[2]}, Min Grade: {result[3]}")

    # Query for excellence rate and failure count per department using CASE WHEN
    cur.execute("""
        SELECT s.Sdept,
               SUM(CASE WHEN sc.Grade >= 90 THEN 1 ELSE 0 END) AS excellent_count,
               SUM(CASE WHEN sc.Grade < 60 THEN 1 ELSE 0 END) AS fail_count,
               COUNT(sc.Grade) AS total_count
        FROM Student s
        JOIN SC sc ON s.Sno = sc.Sno
        GROUP BY s.Sdept
    """)
    stats = cur.fetchall()
    for stat in stats:
        excellent_rate = (stat[1] / stat[3]) * 100 if stat[3] > 0 else 0
        print(f"Department: {stat[0]}, Excellence Rate: {excellent_rate:.2f}%, Number of Failures: {stat[2]}")

    cur.close()
    conn.close()

def rank_students_by_department(department=None):
    """Rank students by department, showing student, course, and grade information."""
    conn = connect_db()
    cur = conn.cursor()
    department = input("Enter department name: ")
    if department != "ALL":
        # 如果指定了系，仅查询该系的学生成绩排名
        query = """
            SELECT s.Sno, s.Sname, c.Cname, sc.Grade
            FROM SC sc
            JOIN Student s ON sc.Sno = s.Sno
            JOIN Course c ON sc.Cno = c.Cno
            WHERE s.Sdept = %s
            ORDER BY sc.Grade DESC
        """
        cur.execute(query, (department,))
    else:
        # 未指定系，则查询所有系的学生成绩排名
        query = """
            SELECT s.Sdept, s.Sno, s.Sname, c.Cname, sc.Grade
            FROM SC sc
            JOIN Student s ON sc.Sno = s.Sno
            JOIN Course c ON sc.Cno = c.Cno
            ORDER BY s.Sdept, sc.Grade DESC
        """
        cur.execute(query)

    results = cur.fetchall()
    if department == "ALL":
        print("Ranking students across all departments:")
    for row in results:
        if department != "ALL":
            print(f"Student No: {row[0]}, Name: {row[1]}, Course: {row[2]}, Grade: {row[3]}")
        else:
            print(f"Department: {row[0]}, Student No: {row[1]}, Name: {row[2]}, Course: {row[3]}, Grade: {row[4]}")

    cur.close()
    conn.close()


def show_student_info_and_courses():
    sno = input("Enter student number: ")
    conn = connect_db()
    cur = conn.cursor()

    # 基本信息
    cur.execute("SELECT * FROM Student WHERE Sno = %s", (sno,))
    student_info = cur.fetchone()
    if student_info:
        print(
            f"Student No: {student_info[0]}, Name: {student_info[1]}, Sex: {student_info[2]}, "
            f"Age: {student_info[3]}, Department: {student_info[4]}, Scholarship: {student_info[5]}")

        # 选课信息
        cur.execute("""
            SELECT c.Cname, sc.Grade
            FROM SC sc
            JOIN Course c ON sc.Cno = c.Cno
            WHERE sc.Sno = %s
        """, (sno,))
        courses = cur.fetchall()
        for course in courses:
            print(f"Course: {course[0]}, Grade: {course[1]}")
    else:
        print("No such student found.")

    cur.close()
    conn.close()

def delete_student():
    """删除学生信息"""
    sno = input("Enter student number to delete: ")
    conn = connect_db()
    cur = conn.cursor()
    cur.execute("DELETE FROM Student WHERE Sno = %s", (sno,))
    conn.commit()
    cur.close()
    conn.close()
    print("Student deleted successfully.")


def main():
    while True:
        print("\n1. Initialize student info")
        print("2. Update course info")
        print("3. Input student grades")
        print("4. caculate student grades by department")
        print("5. Rank students by department")
        print("6. Show Student Info")
        print("7. Exit")
        choice = input("Enter your choice: ")

        if choice == '1':
            initialize_or_update_student_info()
        elif choice == '2':
            update_course_info()
        elif choice == '3':
            input_student_grades()
        elif choice == '4':
            calculate_department_statistics()
        elif choice == '5':
            rank_students_by_department()
        elif choice == '6':
            show_student_info_and_courses()
        elif choice == '7':
            print("Exiting the program.")
            break
        else:
            print("Invalid choice, please try again.")


if __name__ == '__main__':
    main()
