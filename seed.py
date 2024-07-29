from datetime import datetime
import faker
from random import randint, sample
import sqlite3

NUMBER_STUDENTS = 30
NUMBER_GROUPS = 3
NUMBER_SUBJECTES = 8
NUMBER_TEACHERS = 5
NUMBER_GRADES = 20

subjects_list = [
    "Математика",
    "Фізика",
    "Хімія",
    "Біологія",
    "Географія",
    "Історія",
    "Література",
    "Іноземна мова",
    "Фізичне виховання",
    "Мистецтво",
    "Економіка",
    "Право",
    "Психологія",
    "Соціологія",
    "Філософія",
    "Інженерія",
    "Медицина",
    "Музичне мистецтво",
    "Архітектура",
]


def generate_fake_data(number_students, number_groups, number_subjects, number_teachers) -> tuple():

    fake_students = []
    fake_groups = []
    fake_subjects = []
    fake_teachers = []

    fake_data = faker.Faker("uk_UA")

    for _ in range(number_students):
        fake_students.append(fake_data.name())

    for _ in range(number_groups):
        fake_groups.append("Group " + fake_data.color_name())

    fake_subjects = sample(subjects_list, number_subjects)

    for _ in range(number_teachers):
        fake_teachers.append(fake_data.name().upper())

    return fake_students, fake_groups, fake_subjects, fake_teachers


def prepare_data(students, groups, subjects, teachers, grades=[]) -> tuple():

    for_students = []
    for student in students:
        for_students.append((student, randint(1, NUMBER_GROUPS)))

    for_groups = []
    for group in groups:
        for_groups.append((group,))

    for_subjects = []
    for subject in subjects:
        for_subjects.append((subject, randint(1, NUMBER_TEACHERS)))

    for_teachers = []
    for teacher in teachers:
        for_teachers.append((teacher,))

    for_grades = []
    for student in range(1, NUMBER_STUDENTS):
        for _ in range(1, NUMBER_GRADES):
            grade_date = datetime(2023, randint(1, 12), randint(1, 28)).date()
            # student_id  / subject_id  /  grade  / date_of
            for_grades.append(
                (
                    randint(1, NUMBER_STUDENTS),
                    randint(1, NUMBER_SUBJECTES),
                    randint(1, 12),
                    grade_date,
                )
            )

    return for_students, for_groups, for_subjects, for_teachers, for_grades


def insert_data_to_db(students, groups, subjects, teachers, grades) -> None:

    with sqlite3.connect("university.db") as con:

        cur = con.cursor()
        sql_to_groups = """INSERT INTO groups(name)
                               VALUES (?)"""
        cur.executemany(sql_to_groups, groups)

        sql_to_teachers = """INSERT INTO teachers(fullname)
                               VALUES (?)"""
        cur.executemany(sql_to_teachers, teachers)

        sql_to_students = """INSERT INTO students(fullname, group_id)
                               VALUES (?, ?)"""
        cur.executemany(sql_to_students, students)

        sql_to_subjects = """INSERT INTO subjects(name, teacher_id)
                               VALUES (?, ?)"""
        cur.executemany(sql_to_subjects, subjects)

        sql_to_grades = """INSERT INTO grades(student_id, subject_id, grade, grade_date)
                               VALUES (?, ?, ?, ?)"""
        cur.executemany(sql_to_grades, grades)
        con.commit()


if __name__ == "__main__":
    students, groups, subjects, teachers, grades = prepare_data(
        *generate_fake_data(
            NUMBER_STUDENTS, NUMBER_GROUPS, NUMBER_SUBJECTES, NUMBER_TEACHERS
        )
    )
    insert_data_to_db(students, groups, subjects, teachers, grades)
