CREATE TABLE board_games (
    game_id INT PRIMARY KEY,
    name TEXT,
    year_published FLOAT,
    min_players FLOAT,
    max_players FLOAT,
    min_age FLOAT,
    playing_time FLOAT,
    average_rating FLOAT,
    bayes_average FLOAT,
    num_ratings FLOAT,
    complexity FLOAT,
    publisher TEXT
);

CREATE TABLE board_topics (
    game_id INT,
    details_name TEXT,
    details_yearpublished FLOAT,
    details_minplayers FLOAT,
    details_maxplayers FLOAT,
    details_playingtime FLOAT,
    details_minage FLOAT,
    stats_average FLOAT,
    stats_bayesaverage FLOAT,
    stats_numcomments FLOAT,
    stats_usersrated FLOAT,
    topic TEXT
);

CREATE TABLE board_ldaOut_top_documents (
    row_names INT,
    document TEXT,
    topic INT,
    gamma FLOAT
);


CREATE TABLE board_ldaOut_top_terms (
    row_names INT,
    topic INT,
    term TEXT,
    beta FLOAT
);

CREATE TABLE board_ldaOut_topics (
    row_names TEXT,
    topics_bgg_ldaOut INT
);

-------------PROJECT QUERIES------------------
--1:Get first 10 board games
SELECT * FROM board_games LIMIT 10;
--2:Get names and publishers of games published after 2010
SELECT name, publisher 
FROM board_games 
WHERE year_published > 2010;
--3:Find all games suitable for at least 4 players
SELECT name, min_players, max_players 
FROM board_games 
WHERE min_players >= 4;
--4:Find games with average rating above 8
SELECT name, average_rating 
FROM board_games 
WHERE average_rating > 8;
--5:List all topics from board_topics
SELECT DISTINCT topic 
FROM board_topics;
--6:Count of games by publisher
SELECT publisher, COUNT(*) AS total_games 
FROM board_games 
GROUP BY publisher 
ORDER BY total_games DESC;

--7:Average complexity of games by publisher
SELECT publisher, AVG(complexity) AS avg_complexity 
FROM board_games 
GROUP BY publisher 
ORDER BY avg_complexity DESC;
--8:Average rating by year published
SELECT year_published, AVG(average_rating) AS avg_rating 
FROM board_games 
GROUP BY year_published 
ORDER BY year_published;
--9:Max and min playing time per publisher
SELECT publisher, MAX(playing_time) AS max_time, MIN(playing_time) AS min_time 
FROM board_games 
GROUP BY publisher;

--10:Count of games per topic
SELECT topic, COUNT(*) AS games_count 
FROM board_topics 
GROUP BY topic 
ORDER BY games_count DESC;

--11:Join games with topics
SELECT g.name, t.topic, t.stats_average 
FROM board_games g
JOIN board_topics t
ON g.game_id = t.game_id;
--12:Join games with LDA top documents
SELECT g.name, d.document, d.gamma
FROM board_games g
JOIN board_ldaOut_top_documents d
ON g.game_id = d.row_names;
--13:Get top terms per topic with game name
SELECT g.name, t.term, t.beta
FROM board_games g
JOIN board_ldaOut_top_terms t
ON g.game_id = t.row_names;

--14:Games with their topic IDs from LDA topics table

SELECT g.name, l.topics_bgg_ldaOut
FROM board_games g
JOIN board_ldaOut_topics l
ON g.game_id::TEXT = l.row_names;

--15:Games with topics and number of users rated

SELECT g.name, t.topic, t.stats_usersrated
FROM board_games g
JOIN board_topics t
ON g.game_id = t.game_id
ORDER BY t.stats_usersrated DESC;
--16:Top 10 most complex games
SELECT name, complexity 
FROM board_games 
ORDER BY complexity DESC 
LIMIT 10;
--17:Games with playing time less than 30 mins
SELECT name, playing_time 
FROM board_games 
WHERE playing_time < 30;
--18:Games with highest Bayes average rating
SELECT name, bayes_average 
FROM board_games 
ORDER BY bayes_average DESC 
LIMIT 10;
--19:Most rated games
SELECT name, num_ratings 
FROM board_games 
ORDER BY num_ratings DESC 
LIMIT 10;
--20:Games by minimum age ascending
SELECT name, min_age 
FROM board_games 
ORDER BY min_age ASC;
--21:Rank games by average rating
SELECT name, average_rating,
RANK() OVER (ORDER BY average_rating DESC) AS rating_rank
FROM board_games;
--22:Dense rank by complexity
SELECT name, complexity,
DENSE_RANK() OVER (ORDER BY complexity DESC) AS complexity_rank
FROM board_games;

--23:Cumulative number of ratings per publisher
SELECT publisher, name, num_ratings,
SUM(num_ratings) OVER (PARTITION BY publisher ORDER BY num_ratings DESC) AS cum_ratings
FROM board_games;

--24:Average rating over time using window

SELECT year_published, AVG(average_rating) OVER (ORDER BY year_published) AS cumulative_avg
FROM board_games;
--Percentile rank of user ratings
SELECT name, stats_usersrated,
PERCENT_RANK() OVER (ORDER BY stats_usersrated) AS percentile
FROM board_topics;

--Top 5 topics by number of games
SELECT topic, COUNT(game_id) AS num_games
FROM board_topics
GROUP BY topic
ORDER BY num_games DESC
LIMIT 5;
--Average Bayes rating per topic
SELECT topic, AVG(stats_bayesaverage) AS avg_bayes
FROM board_topics
GROUP BY topic
ORDER BY avg_bayes DESC;
--28:Maximum gamma value per document
SELECT document, MAX(gamma) AS max_gamma
FROM board_ldaOut_top_documents
GROUP BY document
ORDER BY max_gamma DESC;

--29:Top 10 terms with highest beta

SELECT term, beta
FROM board_ldaOut_top_terms
ORDER BY beta DESC
LIMIT 10;
--30:Number of documents per topic

SELECT topic, COUNT(document) AS doc_count
FROM board_ldaOut_top_documents
GROUP BY topic
ORDER BY doc_count DESC;

--31:Top terms for top topic by number of games
SELECT t.term, t.beta
FROM board_ldaOut_top_terms t
JOIN (
    SELECT topic, COUNT(*) AS game_count
    FROM board_topics
    GROUP BY topic
    ORDER BY game_count DESC
    LIMIT 1
) top_topic
ON t.topic = top_topic.topic
ORDER BY t.beta DESC;
--32:Top 5 games per topic based on average rating
SELECT t.topic, g.name, g.average_rating
FROM board_topics t
JOIN board_games g
ON t.game_id = g.game_id
WHERE t.topic IS NOT NULL
ORDER BY t.topic, g.average_rating DESC;
--33:Top document per topic by gamma

SELECT topic, document, gamma
FROM (
    SELECT topic, document, gamma,
           ROW_NUMBER() OVER(PARTITION BY topic ORDER BY gamma DESC) AS rn
    FROM board_ldaOut_top_documents
) sub
WHERE rn = 1;

--34:Combine top terms with top documents per topic
SELECT d.topic, d.document, t.term, t.beta
FROM board_ldaOut_top_documents d
JOIN board_ldaOut_top_terms t
ON d.topic = t.topic
ORDER BY d.topic, t.beta DESC;

--35:Count of games per topic with average gamma
SELECT t.topic, COUNT(t.game_id) AS num_games, AVG(d.gamma) AS avg_gamma
FROM board_topics t
JOIN board_ldaOut_top_documents d
ON t.topic = d.topic
GROUP BY t.topic
ORDER BY num_games DESC;
--36:Top 10 most complex games per topic:
SELECT t.topic, g.name, g.complexity
FROM board_topics t
JOIN board_games g
ON t.game_id = g.game_id
ORDER BY t.topic, g.complexity DESC;
--37:Games suitable for 2-4 players
SELECT name, min_players, max_players
FROM board_games
WHERE min_players >= 2 AND max_players <= 4;

--38:Games with high Bayes and high average rating
SELECT name, bayes_average, average_rating
FROM board_games
WHERE bayes_average > 7 AND average_rating > 7;
--39:Top 5 publishers by total games rated
SELECT publisher, SUM(num_ratings) AS total_ratings
FROM board_games
GROUP BY publisher
ORDER BY total_ratings DESC
LIMIT 5;
--40:Top 10 games with highest user ratings
SELECT g.name, t.stats_usersrated
FROM board_games g
JOIN board_topics t
ON g.game_id = t.game_id
ORDER BY t.stats_usersrated DESC
LIMIT 10;
--41:Top 10 terms for highest-rated topic
SELECT t.term, t.beta
FROM board_ldaOut_top_terms t
JOIN (
    SELECT topic, AVG(stats_bayesaverage) AS avg_bayes
    FROM board_topics
    GROUP BY topic
    ORDER BY avg_bayes DESC
    LIMIT 1
) top_topic
ON t.topic = top_topic.topic
ORDER BY t.beta DESC
LIMIT 10;
--42:Average complexity per topic
SELECT t.topic, AVG(g.complexity) AS avg_complexity
FROM board_topics t
JOIN board_games g
ON t.game_id = g.game_id
GROUP BY t.topic;
--43:Top 5 topics with highest average rating
SELECT topic, AVG(stats_average) AS avg_rating
FROM board_topics
GROUP BY topic
ORDER BY avg_rating DESC
LIMIT 5;
--44:Most frequent terms in top documents

SELECT term, COUNT(*) AS frequency
FROM board_ldaOut_top_terms
GROUP BY term
ORDER BY frequency DESC
LIMIT 10;
--Top topic per document based on gamma
SELECT document, topic, gamma
FROM (
    SELECT document, topic, gamma,
           ROW_NUMBER() OVER(PARTITION BY document ORDER BY gamma DESC) AS rn
    FROM board_ldaOut_top_documents
) sub
WHERE rn = 1;

--46:Total games, avg rating, avg complexity per publisher

SELECT publisher, COUNT(*) AS total_games, AVG(average_rating) AS avg_rating, AVG(complexity) AS avg_complexity
FROM board_games
GROUP BY publisher;

--47:Games distribution by minimum age

SELECT min_age, COUNT(*) AS num_games
FROM board_games
GROUP BY min_age
ORDER BY min_age;

--48:Games distribution by playing time ranges
SELECT
  CASE
    WHEN playing_time <= 30 THEN 'Short'
    WHEN playing_time <= 60 THEN 'Medium'
    ELSE 'Long'
  END AS play_duration,
  COUNT(*) AS num_games
FROM board_games
GROUP BY play_duration;
--49:Topic popularity by number of users rated
SELECT topic, SUM(stats_usersrated) AS total_users
FROM board_topics
GROUP BY topic
ORDER BY total_users DESC;

--50:Top 5 topics with highest Bayes average
SELECT topic, AVG(stats_bayesaverage) AS avg_bayes
FROM board_topics
GROUP BY topic
ORDER BY avg_bayes DESC
LIMIT 5;








