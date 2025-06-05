# test_student_analysis.py
def test_grading(spark_session):
    from student_analysis import build_dataframe

    df = build_dataframe(spark_session)
    result = df.filter(df["name"] == "Alice").collect()[0]["grade"]
    assert result == "A"

