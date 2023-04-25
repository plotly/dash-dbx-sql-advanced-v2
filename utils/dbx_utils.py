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
from sqlalchemy.orm import declarative_base, Session
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

    subq = (
        user_session.query(
            case([(users_table.c.gender == "F", "Female")], else_="Male").label("sex"),
            case([(users_table.c.smoker == "N", "Non-smoker")], else_="Smoker").label(
                "smoker"
            ),
            cast(sensors_table.c.timestamp, DATE).label("date"),
            users_table.c.cholestlevs.label("cholesterol"),
            users_table.c.bp.label("bloodpressure"),
            users_table.c.user_id,
            func.sum(sensors_table.c.num_steps * 0.00035).label("num_steps"),
            func.sum(sensors_table.c.miles_walked * 0.0003).label("miles_walked"),
            func.sum(sensors_table.c.calories_burnt * 0.002).label("calories_burnt"),
        )
        .select_from(sensors_table)
        .join(users_table, sensors_table.c.user_id == users_table.c.userid)
        .group_by("sex", "smoker", "date", "user_id", "cholesterol", "bloodpressure")
        .subquery()
    )

    # Define the main query
    query = (
        user_session.query(
            subq.c.date,
            case(
                [(comp == "sex", subq.c.sex), (comp == "smoker", subq.c.smoker)]
            ).label(comp),
            func.avg(yaxis).label(yaxis + "tot"),
        )
        .group_by(
            subq.c.date,
            case([(comp == "sex", subq.c.sex), (comp == "smoker", subq.c.smoker)]),
        )
        .order_by(subq.c.date)
    )
    with engine.begin() as conn:
        df = pd.read_sql_query(query, con=conn)


def get_listofusers():
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

    # with engine.begin() as conn:
    #     df = pd.read_sql_query(query, con=conn)


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

    with engine.begin() as conn:
        df = pd.read_sql_query(query, con=conn)


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

    with engine.begin() as conn:
        df = pd.read_sql_query(query, con=conn)


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

    with engine.begin() as conn:
        df = pd.read_sql_query(query, con=conn)


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

    subq = (
        select(
            [
                case([(users_table.c.gender == "F", "Female")], else_="Male").label(
                    "sex"
                ),
                case(
                    [(users_table.c.smoker == "N", "Non-smoker")], else_="Smoker"
                ).label("Smoker"),
                users_table.c.cholestlevs.label("cholesterol"),
                users_table.c.bp.label("bloodpressure"),
                func.sum(sensors_table.c.num_steps * 0.00035).label("num_steps"),
                func.sum(sensors_table.c.miles_walked * 0.0003).label("miles_walked"),
                func.sum(sensors_table.c.calories_burnt * 0.002).label(
                    "calories_burnt"
                ),
                users_table.c.age,
                users_table.c.height,
                users_table.c.weight,
                sensors_table.c.user_id,
            ]
        )
        .select_from(
            sensors_table.join(
                users_table, sensors_table.c.user_id == users_table.c.userid
            )
        )
        .where(
            fitness.between(
                (
                    select([func.max(sensors_table.c.fitness)]).scalar()
                    * slider[0]
                    * 0.01
                ),
                (
                    select([func.max(sensors_table.c.fitness)]).scalar()
                    * slider[1]
                    * 0.01
                ),
            )
        )
        .group_by(
            case([(users_table.c.gender == "F", "Female")], else_="Male"),
            case([(users_table.c.smoker == "N", "Non-smoker")], else_="Smoker"),
            users_table.c.cholestlevs,
            users_table.c.bp,
            users_table.c.user_id,
            users_table.c.age,
            users_table.c.height,
            users_table.c.weight,
        )
        .alias()
    )

    # define main query
    query = (
        select(
            [
                axis1,
                axis2,
                comp,
                func.avg(fitness).label(f"{fitness}tot"),
            ]
        )
        .select_from(subq)
        .group_by(comp, axis1, axis2)
        .order_by(f"{fitness}tot")
    )

    with engine.begin() as conn:
        df = pd.read_sql_query(query, con=conn)


def get_user_comp(fitness):
    users_table = Table(
        "silver_users",
        utils.ddls.Base.metadata,
        Column("user_id", Integer),
        autoload=True,
        autoload_with=engine,
        extend_existing=True,
    )

    sensors_table = Table(
        "silver_sensors",
        utils.ddls.Base.metadata,
        Column("user_id", Integer),
        autoload=True,
        autoload_with=engine,
        extend_existing=True,
    )

    subq = (
        user_session.query(
            func.sum(sensors_table.num_steps * 0.00035).label("num_steps"),
            func.sum(sensors_table.miles_walked * 0.0003).label("miles_walked"),
            func.sum(sensors_table.calories_burnt * 0.002).label("calories_burnt"),
            sensors_table.users_table_id.label("users_table_id"),
        )
        .select_from(sensors_table)
        .join(users_table, sensors_table.user_id == users_table.userid)
        .group_by(sensors_table.user_id)
        .subquery()
    )

    query = user_session.query(
        subq.c.user_id, subq.c.num_steps + subq.c.miles_walked + subq.c.calories_burnt
    ).order_by(subq.c.num_steps + subq.c.miles_walked + subq.c.calories_burnt)

    with engine.begin() as conn:
        df = pd.read_sql_query(query, con=conn)
