-- CreateTable
CREATE TABLE "Movie" (
    "id" TEXT NOT NULL,
    "title" TEXT NOT NULL,
    "vote_average" DOUBLE PRECISION NOT NULL,
    "vote_count" INTEGER NOT NULL,
    "status" TEXT NOT NULL,
    "release_date" TIMESTAMP(3),
    "revenue" BIGINT NOT NULL,
    "runtime" INTEGER,
    "budget" BIGINT NOT NULL,
    "imdb_id" TEXT,
    "original_language" TEXT,
    "original_title" TEXT NOT NULL,
    "overview" TEXT,
    "popularity" DOUBLE PRECISION,
    "tagline" TEXT,
    "genres" TEXT[],
    "production_companies" JSONB NOT NULL,
    "production_countries" TEXT[],
    "spoken_languages" TEXT[],
    "cast" TEXT[],
    "director" TEXT[],
    "director_of_photography" TEXT[],
    "writers" TEXT[],
    "producers" TEXT[],
    "music_composer" TEXT[],
    "imdb_rating" DOUBLE PRECISION,
    "imdb_votes" INTEGER,
    "poster_path" TEXT,

    CONSTRAINT "Movie_pkey" PRIMARY KEY ("id")
);

-- CreateIndex
CREATE UNIQUE INDEX "Movie_id_key" ON "Movie"("id");

-- CreateIndex
CREATE INDEX "Movie_release_date_idx" ON "Movie"("release_date");

-- CreateIndex
CREATE INDEX "Movie_genres_idx" ON "Movie" USING GIN ("genres");

-- CreateIndex
CREATE INDEX "Movie_vote_average_idx" ON "Movie"("vote_average");
