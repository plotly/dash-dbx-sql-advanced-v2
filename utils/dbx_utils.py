from constants import (
    SERVER_HOSTNAME,
    HTTP_PATH,
    ACCESS_TOKEN,
    DB_NAME,
    SCHEMA,
)
from databricks import sqlalchemy
import sqlalchemy
import datetime
from sqlalchemy.orm import declarative_base, Session, aliased
from sqlalchemy import (
    text,
    Table,
    Column,
    String,
    Integer,
    BOOLEAN,
    DECIMAL,
    TIMESTAMP,
    DATE,
    create_engine,
    select,
    case,
    cast,
    func,
    alias,
    distinct,
    asc,
    MetaData,
    between,
)
import pandas as pd
from utils.ddls import SilverSensors
import utils.ddls

engine = create_engine(
    f"databricks://token:{ACCESS_TOKEN}@{SERVER_HOSTNAME}/?http_path={HTTP_PATH}&catalog={DB_NAME}&schema={SCHEMA}"
)


user_session = Session(bind=engine)
user_base = declarative_base(bind=engine)
metadata = MetaData(bind=engine)


def get_line_data(yaxis, comp):
    users_table = Table(
        "silver_users",
        utils.ddls.Base.metadata,
        autoload=True,
        autoload_with=engine,
        extend_existing=True,
    )

    sensors_table = Table(
        "silver_sensors",
        utils.ddls.Base.metadata,
        autoload=True,
        autoload_with=engine,
        extend_existing=True,
    )
    subquery = (
        select(
            func.CAST(sensors_table.c.timestamp, DATE).label("date"),
            case([(users_table.c.gender == "F", "Female")], else_="Male").label("sex"),
            case([(users_table.c.smoker == "N", "Non-smoker")], else_="Smoker").label(
                "smoker"
            ),
            sensors_table.c.user_id,
            func.sum(sensors_table.c.num_steps * 0.00035).label("num_steps"),
            func.sum(sensors_table.c.miles_walked * 0.0003).label("miles_walked"),
            func.sum(sensors_table.c.calories_burnt * 0.002).label("calories_burnt"),
            users_table.c.cholestlevs.label("cholesterol"),
            users_table.c.bp.label("bloodpressure"),
        )
        .select_from(
            sensors_table.join(
                users_table, sensors_table.c.user_id == users_table.c.userid
            )
        )
        .group_by("date", "sex", "smoker", "user_id", "cholesterol", "bloodpressure")
        .alias("subquery")
    )

    # define the outer query to aggregate the data by date and comp (presumably a comparison metric)
    query = (
        select(
            subquery.c.date,
            case([(subquery.c.sex == "Female", "F")], else_="M").label("sex"),
            case([(subquery.c.smoker == "Smoker", "Y")], else_="N").label("smoker"),
            func.avg(subquery.c.cholesterol).label("cholesteroltot"),
            func.avg(subquery.c.bloodpressure).label("bloodpressuretot"),
            func.avg(subquery.c.num_steps).label("num_stepstot"),
            func.avg(subquery.c.miles_walked).label("miles_walkedtot"),
            func.avg(subquery.c.calories_burnt).label("calories_burnttot"),
        )
        .select_from(subquery)
        .group_by("date", "sex", "smoker")
        .order_by("date")
    )

    df = pd.read_sql_query(query, engine)

    return df


def get_listofusers(dash_prepare=False):
    users_table = Table(
        "silver_users",
        utils.ddls.Base.metadata,
        autoload=True,
        autoload_with=engine,
        extend_existing=True,
    )

    query = (
        user_session.query(users_table.c.userid)
        .distinct()
        .order_by(users_table.c.userid.asc())
    )

    df = pd.read_sql_query(query.statement, engine)

    if dash_prepare:
        return [{"label": str(i), "value": str(i)} for i in df["userid"]]
    else:
        return df


def get_user_data(user):
    users_table = Table(
        "silver_users",
        utils.ddls.Base.metadata,
        autoload=True,
        autoload_with=engine,
        extend_existing=True,
    )

    subq = user_session.query(
        case([(users_table.c.gender == "F", "Female")], else_="Male").label("sex"),
        case([(users_table.c.smoker == "N", "Non-smoker")], else_="Smoker").label(
            "Smoker"
        ),
        users_table.c.cholestlevs.label("cholesterol"),
        users_table.c.bp.label("bloodpressure"),
        users_table.c.userid,
        users_table.c.age,
        users_table.c.height,
        users_table.c.weight,
    ).subquery()

    query = user_session.query(subq).filter(subq.c.userid == user)

    df = pd.read_sql_query(query.statement, engine)

    return df


def get_fitness_data(user, fitness):
    sensors_table = Table(
        "silver_sensors",
        utils.ddls.Base.metadata,
        autoload=True,
        autoload_with=engine,
        extend_existing=True,
    )

    subq = (
        user_session.query(
            cast(sensors_table.c.timestamp, DATE).label("date"),
            (sensors_table.c.num_steps * 0.00035).label("num_steps"),
            (sensors_table.c.miles_walked * 0.0003).label("miles_walked"),
            (sensors_table.c.calories_burnt * 0.002).label("calories_burnt"),
        )
        .filter(sensors_table.c.user_id == user)
        .subquery()
    )

    query = (
        user_session.query(
            subq.c.date, func.sum(getattr(subq.c, fitness)).label(fitness)
        )
        .group_by(subq.c.date)
        .order_by(subq.c.date)
    )

    df = pd.read_sql_query(query.statement, engine)

    return df


def get_scatter_data(xaxis, comp):
    users_table = Table(
        "silver_users",
        utils.ddls.Base.metadata,
        autoload=True,
        autoload_with=engine,
        extend_existing=True,
    )

    subq = user_session.query(
        case([(users_table.c.gender == "F", "Female")], else_="Male").label("sex"),
        users_table.c.age,
        users_table.c.height,
        users_table.c.weight,
        case([(users_table.c.smoker == "N", "Non-smoker")], else_="Smoker").label(
            "Smoker"
        ),
        users_table.c.cholestlevs.label("cholesterol"),
        users_table.c.bp.label("bloodpressure"),
        users_table.c.risk,
        users_table.c.userid,
    ).subquery()

    query = user_session.query(
        getattr(subq.c, xaxis).label(xaxis),
        getattr(subq.c, comp).label(comp),
        subq.c.risk,
        func.count(distinct(subq.c.userid)).label("Total"),
    ).group_by(getattr(subq.c, xaxis), getattr(subq.c, comp), subq.c.risk)

    df = pd.read_sql_query(query.statement, engine)
    return df


def get_heat_data(axis1, axis2, fitness, comp, slider):
    users_table = Table(
        "silver_users",
        utils.ddls.Base.metadata,
        autoload=True,
        autoload_with=engine,
        extend_existing=True,
    )

    sensors_table = Table(
        "silver_sensors",
        utils.ddls.Base.metadata,
        autoload=True,
        autoload_with=engine,
        extend_existing=True,
    )

    query = f"""SELECT {axis1}, {axis2}, {comp}, AVG({fitness}) AS {fitness}tot 
            FROM(
                SELECT
                CASE WHEN gender='F' THEN 'Female' ELSE 'Male' END AS sex, 
                CASE WHEN smoker='N' THEN 'Non-smoker' ELSE 'Smoker' END AS Smoker,
                cholestlevs AS cholesterol, bp AS bloodpressure,
                SUM(num_steps*0.00035) AS num_steps, SUM(miles_walked*0.0003) AS miles_walked, SUM(calories_burnt*0.002) AS calories_burnt,
                age, height, weight, user_id
                FROM {sensors_table}
                LEFT JOIN {users_table} ON {sensors_table}.user_id = {users_table}.userid
                WHERE {fitness} BETWEEN ((SELECT MAX({fitness}) FROM {sensors_table})*{slider[0]}*0.01) 
                AND ((SELECT MAX({fitness}) FROM {sensors_table})*{slider[1]}*0.01)
                GROUP BY sex, Smoker, cholesterol, bloodpressure, user_id, age, height, weight
            )
            GROUP BY {comp}, {axis1}, {axis2}
            """

    # subq = (
    #     select(
    #         [
    #             case([(users_table.c.gender == "F", "Female")], else_="Male").label(
    #                 "sex"
    #             ),
    #             case(
    #                 [(users_table.c.smoker == "N", "Non-smoker")], else_="Smoker"
    #             ).label("Smoker"),
    #             users_table.c.cholestlevs.label("cholesterol"),
    #             users_table.c.bp.label("bloodpressure"),
    #             func.sum(sensors_table.c.num_steps * 0.00035).label("num_steps"),
    #             func.sum(sensors_table.c.miles_walked * 0.0003).label("miles_walked"),
    #             func.sum(sensors_table.c.calories_burnt * 0.002).label(
    #                 "calories_burnt"
    #             ),
    #             users_table.c.age,
    #             users_table.c.height,
    #             users_table.c.weight,
    #             sensors_table.c.user_id,
    #         ]
    #     )
    #     .select_from(
    #         sensors_table.join(
    #             users_table, sensors_table.c.user_id == users_table.c.userid
    #         )
    #     )
    #     .where(
    #         sensors_table.c[fitness].between(
    #             (
    #                 select([func.max(sensors_table.c[fitness])]).scalar()
    #                 * slider[0]
    #                 * 0.01
    #             ),
    #             (
    #                 select([func.max(sensors_table.c[fitness])]).scalar()
    #                 * slider[1]
    #                 * 0.01
    #             ),
    #         )
    #     )
    #     .group_by(
    #         case([(users_table.c.gender == "F", "Female")], else_="Male"),
    #         case([(users_table.c.smoker == "N", "Non-smoker")], else_="Smoker"),
    #         users_table.c.cholestlevs,
    #         users_table.c.bp,
    #         sensors_table.c.user_id,  ##TODO
    #         users_table.c.age,
    #         users_table.c.height,
    #         users_table.c.weight,
    #     )
    #     .alias()
    # ).subquery()

    # # define main query
    # query = (
    #     user_session.query(
    #         [
    #             sensors_table.c[axis1],
    #             sensors_table.c[axis2],
    #             sensors_table.c[comp],
    #             func.avg(sensors_table.c[fitness]).label(f"{fitness}tot"),
    #         ]
    #     )
    #     .select_from(subq)
    #     .group_by(sensors_table.c[comp], sensors_table.c[axis1], sensors_table.c[axis2])
    #     .order_by(f"{fitness}tot")
    # )

    df = pd.read_sql_query(query, engine)

    return df


def get_user_comp(fitness):
    users_table = Table(
        "silver_users",
        utils.ddls.Base.metadata,
        autoload=True,
        autoload_with=engine,
        extend_existing=True,
    )

    sensors_table = Table(
        "silver_sensors",
        utils.ddls.Base.metadata,
        autoload=True,
        autoload_with=engine,
        extend_existing=True,
    )

    subq = (
        user_session.query(
            func.sum(sensors_table.c.num_steps * 0.00035).label("num_steps"),
            func.sum(sensors_table.c.miles_walked * 0.0003).label("miles_walked"),
            func.sum(sensors_table.c.calories_burnt * 0.002).label("calories_burnt"),
            sensors_table.c.user_id,
        )
        .select_from(sensors_table)
        .join(users_table, sensors_table.c.user_id == users_table.c.userid)
        .group_by(sensors_table.c.user_id.label("user_id"))
        .subquery()
    )

    query = user_session.query(
        subq.c.user_id, subq.c.num_steps + subq.c.miles_walked + subq.c.calories_burnt
    ).order_by(subq.c.num_steps + subq.c.miles_walked + subq.c.calories_burnt)

    df = pd.read_sql_query(query.statement, engine)

    return df
