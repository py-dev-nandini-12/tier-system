import json
import logging

from fastapi import FastAPI, HTTPException, Depends
from databases import Database
from sqlalchemy import create_engine, Column, Integer, String, DateTime, ForeignKey, text
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.ext.declarative import declarative_base
import redis
from typing import List
import psycopg2
import uvicorn

DATABASE_URL = "postgresql://nandinichatterjee:postgresql_tutorial@localhost/points"
REDIS_URL = "redis://localhost"

database = Database(DATABASE_URL)
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


# Models
class User(Base):
    __tablename__ = "users"
    username = Column(String, primary_key=True, index=True)
    points = Column(Integer, default=0)
    tier = Column(String, default="Bronze")


class Point(Base):
    __tablename__ = "points"
    id = Column(Integer, primary_key=True, index=True)
    username = Column(String, ForeignKey("users.username"))
    type = Column(String)
    amount = Column(Integer)
    timestamp = Column(DateTime, server_default=text("(now() at time zone 'utc')"), index=True)


# Create tables
Base.metadata.create_all(bind=engine)

# Create a FastAPI app
app = FastAPI()

# Redis setup

redis_client = redis.StrictRedis.from_url(REDIS_URL)


# Dependency to get the database session
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# Get leaderboard from PostgreSQL
def get_leaderboard_from_db(db: Session):
    query = "SELECT * FROM users ORDER BY points DESC LIMIT 5"
    return db.execute(query).fetchall()


# Get leaderboard from Redis (cached)
def get_leaderboard_from_redis():
    leaderboard_cached = redis_client.get("leaderboard")
    if leaderboard_cached:
        # try:
        #     # Decode the string and convert it to a list of User objects
        #     return json.loads(leaderboard_cached.decode("utf-8"))
        # except json.decoder.JSONDecodeError as e:
        #     # Log the error for debugging purposes
        #     logging.error(f"Error decoding JSON: {e}")
        #     # Return a default value or an empty list
        #     return []
        # else:
        #     return None
        # Decode the string and convert it to a list of User objects
        return eval(leaderboard_cached.decode("utf-8"))
    else:
        return None


# Update Redis cache with the current leaderboard
def update_leaderboard_in_redis(leaderboard: List[User]):
    redis_client.set("leaderboard", str(leaderboard))


# Get the current leaderboard (either from cache or database)
def get_current_leaderboard(db: Session = Depends(get_db)):
    leaderboard_cached = get_leaderboard_from_redis()
    if leaderboard_cached:
        return leaderboard_cached
    else:
        leaderboard_db = get_leaderboard_from_db(db)
        update_leaderboard_in_redis(leaderboard_db)
        return leaderboard_db


# Define a function to update the user's tier based on points
def update_tier(db: Session, user: User):
    if user.points < 50:
        new_tier = 'Bronze'
    elif 50 <= user.points <= 100:
        new_tier = 'Silver'
    else:
        new_tier = 'Gold'

    # Create a new User instance with the updated tier
    updated_user = User(username=user.username, points=user.points, tier=new_tier)

    # Update the user's tier in the database
    db.execute(
        "UPDATE users SET tier = :new_tier WHERE username = :username",
        {"new_tier": new_tier, "username": updated_user.username}
    )

    # Commit the changes to the database
    db.commit()

    # Update the user object with the new tier

    updated_user.tier = new_tier


# Endpoint to create a new user
@app.post("/create_user/{username}")
def create_user(username: str, db: Session = Depends(get_db)):
    try:
        # Check if the user already exists
        existing_user = db.query(User).filter(User.username == username).first()
        if existing_user:
            raise HTTPException(
                status_code=400, detail="User with that username already exists"
            )

        # Create a new user
        new_user = User(username=username)
        db.add(new_user)
        db.commit()
        db.refresh(new_user)

        return {"message": f"User {username} created successfully"}

    except Exception as e:
        # Rollback the transaction in case of an error
        db.rollback()
        raise e
    finally:
        # Close the database session
        db.close()


# Endpoint to earn points
@app.post("/earn_points/{username}/{points_type}/{amount}")
def earn_points(
        username: str,
        points_type: str,
        amount: int,
        db: Session = Depends(get_db)):
    try:
        # Check if the user exists
        user = db.query(User).filter(User.username == username).first()
        if not user:
            raise HTTPException(status_code=404, detail="User not found")

        # Insert points into PostgreSQL using the provided session
        query = (
            "INSERT INTO points (username, type, amount) "
            "VALUES (:username, :type, :amount) RETURNING *"
        )
        points_entry = db.execute(
            query,
            {"username": username, "type": points_type, "amount": amount}  # Pass values as a dictionary
        ).fetchone()

        if not points_entry:
            raise HTTPException(
                status_code=400, detail="Failed to add points entry"
            )

        # Update user points in PostgreSQL using the provided session
        query = (
            "UPDATE users SET points = points + :amount "
            "WHERE username = :username RETURNING *"
        )
        user = db.execute(
            query, {"username": username, "amount": amount}  # Pass values as a dictionary
        ).fetchone()

        if not user:
            raise HTTPException(status_code=404, detail="User not found")
            # Update the user's tier based on points
        update_tier(db, user)

        # Invalidate Redis cache for the user
        redis_client.delete(username)

        # Update leaderboard in Redis
        leaderboard = get_leaderboard_from_db(db)
        update_leaderboard_in_redis(leaderboard)

        return {
            "message": f"{amount} points earned by {username}. Current points: {user['points']}."
        }

    except Exception as e:
        # Rollback the transaction in case of an error
        db.rollback()
        raise e
    finally:
        # Commit the transaction
        db.commit()


# Endpoint to get the current leaderboard
@app.get("/leaderboard", response_model=None)
def get_leaderboard(current_leaderboard: List[User] = Depends(get_current_leaderboard)):
    return current_leaderboard


# Run the FastAPI application
if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)
