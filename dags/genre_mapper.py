GENRE_MAP= {
      28 : "액션",
      12 : "모험",
      16 : "애니메이션",
      35 : "코미디",
      80 : "범죄",
      99 : "다큐멘터리",
      18 : "드라마",
      10751 : "가족",
      14 : "판타지",
      36 : "역사",
      27 : "공포",
      10402 : "음악",
      9648 : "미스터리",
      10749 :"로맨스",
      878 : "SF",
      10770 : "TV 영화",
      53 : "스릴러",
      10752 : "전쟁",
      37 : "서부"
}

def preprocess_genres(genre_ids: list[int]) -> list[str]:
    return [GENRE_MAP.get(genre_id, "") for genre_id in genre_ids]

#Example trial, 윗부분을 print로 디버깅 필요하면 사용하셈
#movie_data = <...>
#movie_data["genres"] = preprocess_genres(movie_data["genres"])
#print(movie_data)

from motor.motor_asyncio import AsyncIOMotorClient
import asyncio
 
client = AsyncIOMotorClient("mongodb://root:team3@localhost:27017/")
db = client["movie_db"]
movies_collection = db["movies"]

#장르를 int에서 str로 변경
async def insert_genre_names():
    async for movie in movies_collection.find():
        if "genres" in movie:
            genre_names = preprocess_genres(movie["genres"])
            await movies_collection.update_one(
                {"_id": movie["_id"]}, {"$set": {"genres": genre_names}}
            )

async def main():
    await insert_genre_names()

#asyncio 사용해여 main function 돌리기
if __name__ == "__main__":
    asyncio.run(main())
