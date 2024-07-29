from sqlalchemy import func, desc, and_

from conf.models import Grade, Teacher, Student, Group, Subject
from conf.db import session


def select_01():
    """
    SELECT
        s.id,
        s.fullname,
        ROUND(AVG(g.grade), 2) AS average_grade
    FROM students s
    JOIN grades g ON s.id = g.student_id
    GROUP BY s.id
    ORDER BY average_grade DESC
    LIMIT 5;
    """
    result = (
        session.query(
            Student.id,
            Student.fullname,
            func.round(func.avg(Grade.grade), 2).label("average_grade"),
        )
        .select_from(Student)
        .join(Grade)
        .group_by(Student.id)
        .order_by(desc("average_grade"))
        .limit(5)
        .all()
    )
    return result


def select_02():
    """
    SELECT
        s.id,
        s.fullname,
        ROUND(AVG(g.grade), 2) AS average_grade
    FROM grades g
    JOIN students s ON s.id = g.student_id
    where g.subject_id = 1
    GROUP BY s.id
    ORDER BY average_grade DESC
    LIMIT 1;
    """
    result = (
        session.query(
            Student.id,
            Student.fullname,
            func.round(func.avg(Grade.grade), 2).label("average_grade"),
        )
        .select_from(Grade)
        .join(Student)
        .filter(Grade.subjects_id == 1)
        .group_by(Student.id)
        .order_by(desc("average_grade"))
        .limit(1)
        .all()
    )
    return result


def select_03():
    """
    SELECT grp.id, grp.name , sub.name, ROUND(AVG(gr.grade),2) AS AverageGrade
        FROM students s
        JOIN grades gr ON s.id = gr.student_id
        JOIN subjects sub ON gr.subject_id = sub.id
        JOIN Groups grp ON s.group_id = grp.id
        WHERE sub.id = 3
        GROUP BY grp.id, grp.name, sub.name;
    """
    result = (
        session.query(
            Group.id,
            Group.name,
            Subject.name,
            func.round(func.avg(Grade.grade), 2).label("AverageGrade"),
        )
        .select_from(Student)
        .join(Grade, Grade.student_id == Student.id)
        .join(Subject, Grade.subjects_id == Subject.id)
        .join(Group, Student.group_id == Group.id)
        .filter(Subject.id == 3)
        .group_by(Group.id, Group.name, Subject.name)
        .all()
    )
    return result


def select_04():
    """
    SELECT ROUND(AVG(grade),2) AS AverageGrade FROM grades;
    """
    result = (
        session.query(
            func.round(func.avg(Grade.grade), 2).label("AverageGrade"),
        )
        .select_from(Grade)
        .all()
    )
    return result

def select_05():
    """
    SELECT sub.id , sub.name , t.fullname
    FROM subjects sub
    JOIN teachers t ON sub.teacher_id = t.id
    WHERE t.id = 1;
    """

    result = (
        session.query(
            Subject.id,
            Subject.name,
            Teacher.fullname,
        )
        .select_from(Subject)
        .join(Teacher, Subject.teacher_id == Teacher.id)
        .filter(Teacher.id == 1)
        .all()
    )
    return result


def select_06():
    """
    SELECT s.id , s.fullname
    FROM students s
    JOIN groups g ON s.group_id = g.id
    WHERE g.id = 1;
    """

    result = (
        session.query(
            Student.id,
            Student.fullname,
        )
        .select_from(Student)
        .join(Group, Student.group_id == Group.id)
        .filter(Group.id == 1)
        .all()
    )
    return result

def select_07():
    """
    SELECT s.id , s.fullname , sb.name , grd.grade
    FROM students s
    JOIN groups g ON s.group_id = g.id
    JOIN grades grd ON s.id = grd.student_id
    JOIN subjects sb ON sb.id = grd.subject_id
    WHERE g.id = 1 AND sb.id = 2
    GROUP BY s.id, s.fullname , sb.name;
    """

    result = (
        session.query(
            Student.id,
            Student.fullname,
            Subject.name,
            Grade.grade,
        )
        .select_from(Student)
        .join(Group, Student.group_id == Group.id)
        .join(Grade, Student.id == Grade.student_id)
        .join(Subject, Subject.id == Grade.subjects_id)
        .where(and_(Group.id == 1), (Subject.id == 2))
        .all()
    )
    return result


def select_08():
    """
    SELECT t.id , t.fullname , s.name , ROUND(AVG(g.grade),2) AS AverageGrade
    FROM teachers t
    JOIN subjects s ON t.id = s.teacher_id
    JOIN grades g ON s.id = g.subject_id
    GROUP BY t.id , t.fullname , s.name
    ORDER BY AverageGrade DESC;
    """
    result = (
        session.query(
            Teacher.id,
            Teacher.fullname,
            Subject.name,
            func.round(func.avg(Grade.grade), 2).label("AverageGrade"),
        )
        .select_from(Teacher)
        .join(Subject, Teacher.id == Subject.teacher_id)
        .join(Grade, Subject.id == Grade.subjects_id)
        .group_by(Teacher.id, Teacher.fullname, Subject.name)
        .order_by(desc("AverageGrade"))
        .all()
    )
    return result


def select_09():
    """
    SELECT s.id, s.fullname , sb.name
    FROM students s
    JOIN grades g ON s.id = g.student_id
    JOIN subjects sb ON sb.id = g.subject_id
    WHERE s.id = 2
    GROUP BY s.id, s.fullname, sb.name;
    """

    result = (
        session.query(
            Student.id,
            Student.fullname,
            Subject.name,
        )
        .select_from(Student)
        .join(Grade, Student.id == Grade.student_id)
        .join(Subject, Subject.id == Grade.subjects_id)
        .filter(Student.id == 2)
        .group_by(Student.id, Student.fullname, Subject.name)
        .all()
    )
    return result


def select_10():
    """
    SELECT s.id, s.fullname , sb.name , t.fullname AS TeacherFullName
    FROM students s
    JOIN grades g ON s.id = g.student_id
    JOIN subjects sb ON sb.id = g.subject_id
    JOIN teachers t ON t.id = sb.teacher_id
    WHERE s.id = 5 AND t.id = 3
    GROUP BY s.id, s.fullname , sb.name ;
    """

    result = (
        session.query(
            Student.id,
            Student.fullname,
            Subject.name,
            Teacher.fullname.label("TeacherFullName"),
        )
        .select_from(Student)
        .join(Grade, Student.id == Grade.student_id)
        .join(Subject, Subject.id == Grade.subjects_id)
        .join(Teacher, Teacher.id == Subject.teacher_id)
        .where(and_(Student.id == 5), (Teacher.id == 3))
        .group_by(Student.id, Student.fullname, Subject.name)
        .all()
    )
    return result


if __name__ == "__main__":
    print(select_01())
    print(select_02())
    print(select_03())
    print(select_04())
    print(select_05())
    print(select_06())
    print(select_07())
    print(select_08())
    print(select_09())
    print(select_10())
