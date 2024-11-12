USE imdb;

/* Now that you have imported the data sets, let’s explore some of the tables. 
 To begin with, it is beneficial to know the shape of the tables and whether any column has null values.
 Further in this segment, you will take a look at 'movies' and 'genre' tables.*/



-- Segment 1:

-- Q1. Find the total number of rows in each table of the schema?
-- Type your code below:

SELECT COUNT(*)
FROM director_mapping;
-- 3867

SELECT COUNT(*)
FROM movie;
-- 7997

SELECT COUNT(*)
FROM genre;
-- 14662

SELECT COUNT(*)
FROM role_mapping;
-- 15615

SELECT COUNT(*)
FROM ratings;
-- 7997

SELECT COUNT(*)
FROM names;
-- 25735



-- Q2. Which columns in the movie table have null values?
-- Type your code below:

SELECT *
FROM movie;

SELECT
(SELECT COUNT(*) FROM movie WHERE id IS NULL) AS id,
(SELECT COUNT(*) FROM movie WHERE title IS NULL) AS title,
(SELECT COUNT(*) FROM movie WHERE year IS NULL) AS year,
(SELECT COUNT(*) FROM movie WHERE date_published IS NULL) AS date_published,
(SELECT COUNT(*) FROM movie WHERE duration IS NULL) AS duration,
(SELECT COUNT(*) FROM movie WHERE country IS NULL) AS country,
(SELECT COUNT(*) FROM movie WHERE worlwide_gross_income IS NULL) AS worldwide,
(SELECT COUNT(*) FROM movie WHERE languages IS NULL) AS languages,
(SELECT COUNT(*) FROM movie WHERE production_company IS NULL) AS production_company;
;

-- first 5 columns have no null values
-- country has 20 null values
-- worlwide_gross_income has 3724 null values
-- languages has 194 null values
-- production_company has 528 null values





-- Now as you can see four columns of the movie table has null values. Let's look at the at the movies released each year. 
-- Q3. Find the total number of movies released each year? How does the trend look month wise? (Output expected)

/* Output format for the first part:

+---------------+-------------------+
| Year			|	number_of_movies|
+-------------------+----------------
|	2017		|	2134			|
|	2018		|		.			|
|	2019		|		.			|
+---------------+-------------------+


Output format for the second part of the question:
+---------------+-------------------+
|	month_num	|	number_of_movies|
+---------------+----------------
|	1			|	 134			|
|	2			|	 231			|
|	.			|		.			|
+---------------+-------------------+ */
-- Type your code below:

SELECT year , COUNT(year) AS num_of_movies
FROM movie
GROUP BY year;

-- 2017: 3052 , 2018: 2944 , 2019: 2001

SELECT MONTH(date_published) AS month_num , COUNT(title) AS num_of_movies
FROM movie
GROUP BY month_num
ORDER BY month_num DESC;

-- 12: 438 , 11: 625 , 10: 801 , 09: 809 , 08: 678 , 07: 493 , 06: 580 , 05: 625 , 04: 680 , 03: 824 , 02: 640 , 01: 804



/*The highest number of movies is produced in the month of March.
So, now that you have understood the month-wise trend of movies, let’s take a look at the other details in the movies table. 
We know USA and India produces huge number of movies each year. Lets find the number of movies produced by USA or India for the last year.*/
  
-- Q4. How many movies were produced in the USA or India in the year 2019??
-- Type your code below:

SELECT COUNT(DISTINCT id) AS num_of_movies, year
FROM movie
WHERE (country LIKE "%India%" OR country LIKE "%USA%" ) AND year = "2019";


-- 1059




-- Q5. Find the unique list of the genres present in the data set?
-- Type your code below:

SELECT DISTINCT(genre) FROM genre;

-- Drama,Fantasy,Thriller,Comedy,Horror,Family,Romance,Adventure,Action,Sci-Fi,Crime,Mystery,Others



/* So, RSVP Movies plans to make a movie of one of these genres.
Now, wouldn’t you want to know which genre had the highest number of movies produced in the last year?
Combining both the movie and genres table can give more interesting insights. */

-- Q6.Which genre had the highest number of movies produced overall?
-- Type your code below:

SELECT * FROM genre;
SELECT * FROM movie; 

SELECT COUNT(g.genre) AS highest_no_of_movies , g.genre
FROM genre g
INNER JOIN movie m 
ON g.movie_id = m.id
GROUP BY g.genre
ORDER BY highest_no_of_movies DESC;

-- Drama: 4285



/* So, based on the insight that you just drew, RSVP Movies should focus on the ‘Drama’ genre. 
But wait, it is too early to decide. A movie can belong to two or more genres. 
So, let’s find out the count of movies that belong to only one genre.*/

-- Q7. How many movies belong to only one genre?
-- Type your code below:

SELECT genre_count , COUNT(movie_id) AS movie_count
FROM ( SELECT movie_id , COUNT(genre) AS genre_count
	   FROM genre
	   GROUP BY movie_id
       ORDER BY genre_count DESC) AS genre_counts
WHERE genre_count = 1
GROUP BY genre_count;


-- 3289 movies belong to only one genre count


/* There are more than three thousand movies which has only one genre associated with them.
So, this figure appears significant. 
Now, let's find out the possible duration of RSVP Movies’ next project.*/

-- Q8.What is the average duration of movies in each genre? 
-- (Note: The same movie can belong to multiple genres.)


/* Output format:

+---------------+-------------------+
| genre			|	avg_duration	|
+-------------------+----------------
|	thriller	|		105			|
|	.			|		.			|
|	.			|		.			|
+---------------+-------------------+ */
-- Type your code below:


SELECT g.genre , AVG(m.duration) AS avg_duration
FROM genre g
INNER JOIN movie m
ON g.movie_id = m.id 
GROUP BY g.genre
ORDER BY avg_duration DESC;

-- Duration for Action movies is the highest with duration of 112.8 mins whereas Horror movies have the least with the duration of 97.92 mins.

/* Now you know, movies of genre 'Drama' (produced highest in number in 2019) has the average duration of 106.77 mins.
Lets find where the movies of genre 'thriller' on the basis of number of movies.*/

-- Q9.What is the rank of the ‘thriller’ genre of movies among all the genres in terms of number of movies produced? 
-- (Hint: Use the Rank function)


/* Output format:
+---------------+-------------------+---------------------+
| genre			|		movie_count	|		genre_rank    |	
+---------------+-------------------+---------------------+
|drama			|	2312			|			2		  |
+---------------+-------------------+---------------------+*/
-- Type your code below:

SELECT genre , COUNT(movie_id) AS movie_count , RANK()  OVER(PARTITION BY genre) AS genre_rank
FROM genre
WHERE genre = "thriller"
GROUP BY genre;


-- the genre "Thriller" has a movie count over 1484 and ranked 1 in terms of the number of movies produced 






/*Thriller movies is in top 3 among all genres in terms of number of movies
 In the previous segment, you analysed the movies and genres tables. 
 In this segment, you will analyse the ratings table as well.
To start with lets get the min and max values of different columns in the table*/


-- Segment 2:

-- Q10.  Find the minimum and maximum values in  each column of the ratings table except the movie_id column?
/* Output format:
+---------------+-------------------+---------------------+----------------------+-----------------+-----------------+
| min_avg_rating|	max_avg_rating	|	min_total_votes   |	max_total_votes 	 |min_median_rating|min_median_rating|
+---------------+-------------------+---------------------+----------------------+-----------------+-----------------+
|		0		|			5		|	       177		  |	   2000	    		 |		0	       |	8			 |
+---------------+-------------------+---------------------+----------------------+-----------------+-----------------+*/
-- Type your code below:

SELECT * FROM ratings;

SELECT MIN(avg_rating) AS min_avg_rating , MAX(avg_rating) AS max_avg_rating , MIN(total_votes) AS min_total_votes , MAX(total_votes) AS max_total_votes,
MIN(median_rating) AS min_median_rating , MAX(median_rating) AS max_median_rating
FROM ratings;

-- min_avg_rating = 1.0
-- max_avg_rating = 10.0
-- min_total_votes = 100
-- max_total_votes = 725138
-- min_median_rating = 1
-- max_median_rating = 10

    

/* So, the minimum and maximum values in each column of the ratings table are in the expected range. 
This implies there are no outliers in the table. 
Now, let’s find out the top 10 movies based on average rating.*/

-- Q11. Which are the top 10 movies based on average rating?
/* Output format:
+---------------+-------------------+---------------------+
| title			|		avg_rating	|		movie_rank    |
+---------------+-------------------+---------------------+
| Fan			|		9.6			|			5	  	  |
|	.			|		.			|			.		  |
|	.			|		.			|			.		  |
|	.			|		.			|			.		  |
+---------------+-------------------+---------------------+*/
-- Type your code below:
-- It's ok if RANK() or DENSE_RANK() is used too


SELECT m.title , r.avg_rating , ROW_NUMBER() OVER( ORDER BY r.avg_rating DESC ) AS movie_rank
FROM ratings r 
INNER JOIN movie m
ON r.movie_id = m.id
LIMIT 10;


-- check after running the command above


/* Do you find you favourite movie FAN in the top 10 movies with an average rating of 9.6? If not, please check your code again!!
So, now that you know the top 10 movies, do you think character actors and filler actors can be from these movies?
Summarising the ratings table based on the movie counts by median rating can give an excellent insight.*/

-- Q12. Summarise the ratings table based on the movie counts by median ratings.
/* Output format:

+---------------+-------------------+
| median_rating	|	movie_count		|
+-------------------+----------------
|	1			|		105			|
|	.			|		.			|
|	.			|		.			|
+---------------+-------------------+ */
-- Type your code below:
-- Order by is good to have

SELECT * FROM ratings;

SELECT median_rating , COUNT(movie_id)
FROM ratings
GROUP BY median_rating
ORDER BY median_rating ASC;


-- Movies with a median rating of 7 has the highet movie_count



/* Movies with a median rating of 7 is highest in number. 
Now, let's find out the production house with which RSVP Movies can partner for its next project.*/

-- Q13. Which production house has produced the most number of hit movies (average rating > 8)??
/* Output format:
+------------------+-------------------+---------------------+
|production_company|movie_count	       |	prod_company_rank|
+------------------+-------------------+---------------------+
| The Archers	   |		1		   |			1	  	 |
+------------------+-------------------+---------------------+*/
-- Type your code below:

SELECT * FROM movie;

SELECT m.production_company , COUNT(m.id) AS movie_count , ROW_NUMBER() OVER( ORDER BY COUNT(m.id) DESC ) AS prod_company_rank
FROM movie m
INNER JOIN ratings r 
ON m.id = r.movie_id
WHERE r.avg_rating > 8 AND m.production_company IS NOT NULL
GROUP BY m.production_company LIMIT 3;

-- Dreamm Warrior Pictures, Naational Theatre Live are the production houses with most number of movie hits,i.e, 3 , with an avg rating of more than 8





-- It's ok if RANK() or DENSE_RANK() is used too
-- Answer can be Dream Warrior Pictures or National Theatre Live or both

-- Q14. How many movies released in each genre during March 2017 in the USA had more than 1,000 votes?
/* Output format:

+---------------+-------------------+
| genre			|	movie_count		|
+-------------------+----------------
|	thriller	|		105			|
|	.			|		.			|
|	.			|		.			|
+---------------+-------------------+ */
-- Type your code below:

SELECT * FROM movie;
SELECT * FROM genre;
SELECT * FROM ratings;


SELECT g.genre, COUNT(m.id) AS movie_count
FROM movie m 
INNER JOIN genre g
ON m.id = g.movie_id
INNER JOIN ratings r 
ON g.movie_id = r.movie_id
WHERE (m.year = 2017) AND (country LIKE "%USA%") AND (r.total_votes > 1000) AND MONTH(date_published) = 3
GROUP BY g.genre
ORDER BY movie_count DESC;

-- Drama = 24
-- Comedy = 9
-- Action = 8
-- Thriller = 8
-- Sci-fi = 7
-- Crime = 6
-- Horror = 6
-- Mystery = 4
-- Romance = 4
-- Fantasy = 3
-- Adventure = 3
-- Family = 1





-- Lets try to analyse with a unique problem statement.
-- Q15. Find movies of each genre that start with the word ‘The’ and which have an average rating > 8?
/* Output format:
+---------------+-------------------+---------------------+
| title			|		avg_rating	|		genre	      |
+---------------+-------------------+---------------------+
| Theeran		|		8.3			|		Thriller	  |
|	.			|		.			|			.		  |
|	.			|		.			|			.		  |
|	.			|		.			|			.		  |
+---------------+-------------------+---------------------+*/
-- Type your code below:

SELECT m.title, r.avg_rating, g.genre
FROM movie m
INNER JOIN genre g
ON m.id = g.movie_id
INNER JOIN ratings r
ON g.movie_id = r.movie_id
WHERE (m.title LIKE "The%") AND (avg_rating > 8)
ORDER BY r.avg_rating DESC;


-- There are 15 movies starting with the word "The" and an avg_rating above 8






-- You should also try your hand at median rating and check whether the ‘median rating’ column gives any significant insights.
-- Q16. Of the movies released between 1 April 2018 and 1 April 2019, how many were given a median rating of 8?
-- Type your code below:


SELECT * FROM movie;
SELECT * FROM ratings;


SELECT COUNT(movie_id) AS movie_count
FROM ratings r
INNER JOIN movie m 
ON r.movie_id = m.id
WHERE (date_published BETWEEN "2018-04-01" AND "2019-04-01")  AND (median_rating = 8);

-- There were 361 movies released between  1 April 2018 and 1 April 2019 with a median_rating of 8




-- Once again, try to solve the problem given below.
-- Q17. Do German movies get more votes than Italian movies? 
-- Hint: Here you have to find the total number of votes for both German and Italian movies.
-- Type your code below:

SELECT * FROM movie;

SELECT m.country, COUNT(r.total_votes) AS Total_votes
FROM movie m
INNER JOIN ratings r
ON m.id = r.movie_id
WHERE (country = "Germany" OR country = "Italy")
GROUP BY country; 

-- German movies have 146 votes whereas Italian movies have 123 votes


-- Answer is Yes




/* Now that you have analysed the movies, genres and ratings tables, let us now analyse another table, the names table. 
Let’s begin by searching for null values in the tables.*/

-- Segment 3:

-- Q18. Which columns in the names table have null values??
/*Hint: You can find null values for individual columns or follow below output format
+---------------+-------------------+---------------------+----------------------+
| name_nulls	|	height_nulls	|date_of_birth_nulls  |known_for_movies_nulls|
+---------------+-------------------+---------------------+----------------------+
|		0		|			123		|	       1234		  |	   12345	    	 |
+---------------+-------------------+---------------------+----------------------+*/
-- Type your code below:

SELECT * FROM names;

SELECT
(SELECT COUNT(*) FROM names WHERE name IS NULL) AS name_nulls,
(SELECT COUNT(*) FROM names WHERE height IS NULL) AS height_nulls,
(SELECT COUNT(*) FROM names WHERE date_of_birth IS NULL) AS DOB_nulls,
(SELECT COUNT(*) FROM names WHERE known_for_movies IS NULL) AS known_for_movies_nulls ;

-- HEIGHT_NULLS = 17,335
-- DOB_NULLS = 13,431
-- KNOWN_FOR_MOVIES_NULSS = 15,226






/* There are no Null value in the column 'name'.
The director is the most important person in a movie crew. 
Let’s find out the top three directors in the top three genres who can be hired by RSVP Movies.*/

-- Q19. Who are the top three directors in the top three genres whose movies have an average rating > 8?
-- (Hint: The top three genres would have the most number of movies with an average rating > 8.)
/* Output format:

+---------------+-------------------+
| director_name	|	movie_count		|
+---------------+-------------------|
|James Mangold	|		4			|
|	.			|		.			|
|	.			|		.			|
+---------------+-------------------+ */
-- Type your code below:

SELECT * FROM director_mapping;
SELECT * FROM names;
SELECT * FROM ratings;

SELECT n.name AS director_name, COUNT(d.movie_id) AS movie_count
FROM names n
INNER JOIN director_mapping d
ON n.id = d.name_id
INNER JOIN ratings r
ON d.movie_id = r.movie_id
WHERE r.avg_rating > 8 
GROUP BY director_name
ORDER BY movie_count DESC
LIMIT 3;

-- Joe russo, Anthony Russo and James Mangold


/* James Mangold can be hired as the director for RSVP's next project. Do you remeber his movies, 'Logan' and 'The Wolverine'. 
Now, let’s find out the top two actors.*/

-- Q20. Who are the top two actors whose movies have a median rating >= 8?
/* Output format:

+---------------+-------------------+
| actor_name	|	movie_count		|
+-------------------+----------------
|Christain Bale	|		10			|
|	.			|		.			|
+---------------+-------------------+ */
-- Type your code below:

SELECT * FROM role_mapping;
SELECT * FROM names;
SELECT * FROM ratings;

SELECT n.name AS actor_name, COUNT(ro.movie_id) AS movie_count
FROM names n 
INNER JOIN role_mapping ro
ON n.id = ro.name_id
INNER JOIN ratings r 
ON ro.movie_id = r.movie_id
WHERE r.median_rating >= 8
GROUP BY actor_name
ORDER BY movie_count DESC LIMIT 2;

-- Manmmootty 8
-- Mohanlal 5


/* Have you find your favourite actor 'Mohanlal' in the list. If no, please check your code again. 
RSVP Movies plans to partner with other global production houses. 
Let’s find out the top three production houses in the world.*/

-- Q21. Which are the top three production houses based on the number of votes received by their movies?
/* Output format:
+------------------+--------------------+---------------------+
|production_company|vote_count			|		prod_comp_rank|
+------------------+--------------------+---------------------+
| The Archers		|		830			|		1	  		  |
|	.				|		.			|			.		  |
|	.				|		.			|			.		  |
+-------------------+-------------------+---------------------+*/
-- Type your code below:

SELECT * FROM movie;
SELECT * FROM ratings;

SELECT m.production_company, SUM(r.total_votes) AS vote_count, ROW_NUMBER() OVER(ORDER BY SUM(r.total_votes) DESC) AS prod_comp_rank
FROM movie m
INNER JOIN ratings r 
ON m.id = r.movie_id
GROUP BY m.production_company
ORDER BY prod_comp_rank ASC LIMIT 5;

-- Marvel Studios
-- Twentieth Century Fox
-- Warner Bros.




/*Yes Marvel Studios rules the movie world.
So, these are the top three production houses based on the number of votes received by the movies they have produced.

Since RSVP Movies is based out of Mumbai, India also wants to woo its local audience. 
RSVP Movies also wants to hire a few Indian actors for its upcoming project to give a regional feel. 
Let’s find who these actors could be.*/

-- Q22. Rank actors with movies released in India based on their average ratings. Which actor is at the top of the list?
-- Note: The actor should have acted in at least five Indian movies. 
-- (Hint: You should use the weighted average based on votes. If the ratings clash, then the total number of votes should act as the tie breaker.)

/* Output format:
+---------------+-------------------+---------------------+----------------------+-----------------+
| actor_name	|	total_votes		|	movie_count		  |	actor_avg_rating 	 |actor_rank	   |
+---------------+-------------------+---------------------+----------------------+-----------------+
|	Yogi Babu	|			3455	|	       11		  |	   8.42	    		 |		1	       |
|		.		|			.		|	       .		  |	   .	    		 |		.	       |
|		.		|			.		|	       .		  |	   .	    		 |		.	       |
|		.		|			.		|	       .		  |	   .	    		 |		.	       |
+---------------+-------------------+---------------------+----------------------+-----------------+*/
-- Type your code below:

SELECT * FROM movie;
SELECT * FROM ratings;
SELECT * FROM role_mapping;
SELECT * FROM names;

WITH actor_summary 
AS ( SELECT n.name AS actor_name, COUNT(r.movie_id) AS movie_count,
     ROUND(SUM(avg_rating * total_votes) / SUM(total_votes)) AS actor_avg_rank
     FROM movie m 
     INNER JOIN ratings r 
     ON m.id = r.movie_id
     INNER JOIN role_mapping ro 
     ON r.movie_id = ro.movie_id
     INNER JOIN names n 
     ON ro.name_id = n.id
     WHERE category = "actor" AND country = "India"
     GROUP BY actor_name
     HAVING movie_count >=5 
     )
     SELECT *,
     ROW_NUMBER() OVER( ORDER BY actor_avg_rank DESC) AS actor_rank
     FROM actor_summary;






-- Top actor is Vijay Sethupathi

-- Q23.Find out the top five actresses in Hindi movies released in India based on their average ratings? 
-- Note: The actresses should have acted in at least three Indian movies. 
-- (Hint: You should use the weighted average based on votes. If the ratings clash, then the total number of votes should act as the tie breaker.)
/* Output format:
+---------------+-------------------+---------------------+----------------------+-----------------+
| actress_name	|	total_votes		|	movie_count		  |	actress_avg_rating 	 |actress_rank	   |
+---------------+-------------------+---------------------+----------------------+-----------------+
|	Tabu		|			3455	|	       11		  |	   8.42	    		 |		1	       |
|		.		|			.		|	       .		  |	   .	    		 |		.	       |
|		.		|			.		|	       .		  |	   .	    		 |		.	       |
|		.		|			.		|	       .		  |	   .	    		 |		.	       |
+---------------+-------------------+---------------------+----------------------+-----------------+*/
-- Type your code below:

SELECT * FROM movie;

WITH actress_summary AS
(
SELECT n.name AS actress_name, COUNT(r.movie_id) AS movie_count, ROUND( SUM(avg_rating * total_votes) / SUM(total_votes) , 2) AS actress_avg_rating
FROM movie m
INNER JOIN ratings r
ON m.id = r.movie_id
INNER JOIN role_mapping ro 
ON r.movie_id = ro.movie_id
INNER JOIN names n 
ON ro.name_id = n.id
WHERE category = "Actress" AND country = "India" AND languages LIKE "%Hindi%"
GROUP BY actress_name
HAVING movie_count >= 3
)
SELECT *,
ROW_NUMBER() OVER( ORDER BY actress_avg_rating DESC) AS actress_rank
FROM actress_summary LIMIT 7;


-- Taapsee Pannu
-- Kriti Sanon
-- Divya Dutta
-- Shraddha Kapoor




/* Taapsee Pannu tops with average rating 7.74. 
Now let us divide all the thriller movies in the following categories and find out their numbers.*/


/* Q24. Select thriller movies as per avg rating and classify them in the following category: 

			Rating > 8: Superhit movies
			Rating between 7 and 8: Hit movies
			Rating between 5 and 7: One-time-watch movies
			Rating < 5: Flop movies
--------------------------------------------------------------------------------------------*/
-- Type your code below:

SELECT * FROM genre;
SELECT * FROM ratings;

WITH Thriller_movies AS
(
SELECT m.title, r.avg_rating
FROM movie m
INNER JOIN ratings r 
ON m.id = r.movie_id
INNER JOIN genre g
ON r.movie_id = g.movie_id
WHERE g.genre = "Thriller"
)
SELECT *,
CASE 
WHEN avg_rating > 8 THEN "SuperHit Movies"
WHEN avg_rating BETWEEN 7 AND 8 THEN "Hit Movies"
WHEN avg_rating BETWEEN 5 AND 7 THEN "One-Time-watch-Movies"
WHEN avg_rating < 5 THEN "Flop Movies"
END AS Rating_Category
FROM Thriller_movies;







/* Until now, you have analysed various tables of the data set. 
Now, you will perform some tasks that will give you a broader understanding of the data in this segment.*/

-- Segment 4:

-- Q25. What is the genre-wise running total and moving average of the average movie duration? 
-- (Note: You need to show the output table in the question.) 
/* Output format:
+---------------+-------------------+---------------------+----------------------+
| genre			|	avg_duration	|running_total_duration|moving_avg_duration  |
+---------------+-------------------+---------------------+----------------------+
|	comdy		|			145		|	       106.2	  |	   128.42	    	 |
|		.		|			.		|	       .		  |	   .	    		 |
|		.		|			.		|	       .		  |	   .	    		 |
|		.		|			.		|	       .		  |	   .	    		 |
+---------------+-------------------+---------------------+----------------------+*/
-- Type your code below:


SELECT * FROM genre;
SELECT * FROM movie;

WITH Movie_duration AS
(
SELECT g.genre, AVG(m.duration) AS avg_duration
FROM movie m
INNER JOIN genre g
ON m.id = g.movie_id 
GROUP BY genre
)
SELECT *,
SUM(avg_duration) OVER( ORDER BY genre) AS running_total_duration,
AVG(avg_duration) OVER( ORDER BY genre) AS moving_average_duration
FROM Movie_duration;





-- Round is good to have and not a must have; Same thing applies to sorting


-- Let us find top 5 movies of each year with top 3 genres.

-- Q26. Which are the five highest-grossing movies of each year that belong to the top three genres? 
-- (Note: The top 3 genres would have the most number of movies.)

/* Output format:
+---------------+-------------------+---------------------+----------------------+-----------------+
| genre			|	year			|	movie_name		  |worldwide_gross_income|movie_rank	   |
+---------------+-------------------+---------------------+----------------------+-----------------+
|	comedy		|			2017	|	       indian	  |	   $103244842	     |		1	       |
|		.		|			.		|	       .		  |	   .	    		 |		.	       |
|		.		|			.		|	       .		  |	   .	    		 |		.	       |
|		.		|			.		|	       .		  |	   .	    		 |		.	       |
+---------------+-------------------+---------------------+----------------------+-----------------+*/
-- Type your code below:

-- Top 3 Genres based on most number of movies

SELECT * FROM movie;
SELECT * FROM genre;

WITH Top_genres AS
(
SELECT genre, COUNT(m.id) as movie_count, RANK() OVER( ORDER BY COUNT(m.id) DESC) AS genre_rank
FROM movie m
INNER JOIN genre g
ON m.id = g.movie_id
GROUP BY genre LIMIT 3
),
 movie_summary AS
(
SELECT genre, year, title AS movie_name, worlwide_gross_income, DENSE_RANK() OVER( PARTITION BY year ORDER BY worlwide_gross_income DESC) AS movie_rank 
FROM movie m
INNER JOIN genre g 
ON m.id = g.movie_id
WHERE genre IN (SELECT genre FROM Top_genres)
GROUP BY genre
)
SELECT * FROM movie_summary
WHERE movie_rank<=5
ORDER BY year;










-- Finally, let’s find out the names of the top two production houses that have produced the highest number of hits among multilingual movies.
-- Q27.  Which are the top two production houses that have produced the highest number of hits (median rating >= 8) among multilingual movies?
/* Output format:
+-------------------+-------------------+---------------------+
|production_company |movie_count		|		prod_comp_rank|
+-------------------+-------------------+---------------------+
| The Archers		|		830			|		1	  		  |
|	.				|		.			|			.		  |
|	.				|		.			|			.		  |
+-------------------+-------------------+---------------------+*/
-- Type your code below:

SELECT * FROM movie;
SELECT * FROM ratings;

SELECT production_company, COUNT(m.id) AS movie_count, DENSE_RANK() OVER( ORDER BY COUNT(m.id) DESC) AS prod_comp_rank
FROM movie m 
INNER JOIN ratings r
ON m.id = r.movie_id
WHERE POSITION( "," IN m.languages) > 0 AND (median_rating >= 8) AND production_company IS NOT NULL
GROUP BY production_company
ORDER BY prod_comp_rank LIMIT 3;
        


-- Star Cinema : 1
-- Twentieth Century Fox : 2
-- Columbia Pictures : 3








-- Multilingual is the important piece in the above question. It was created using POSITION(',' IN languages)>0 logic
-- If there is a comma, that means the movie is of more than one language


-- Q28. Who are the top 3 actresses based on number of Super Hit movies (average rating >8) in drama genre?
/* Output format:
+---------------+-------------------+---------------------+----------------------+-----------------+
| actress_name	|	total_votes		|	movie_count		  |actress_avg_rating	 |actress_rank	   |
+---------------+-------------------+---------------------+----------------------+-----------------+
|	Laura Dern	|			1016	|	       1		  |	   9.60			     |		1	       |
|		.		|			.		|	       .		  |	   .	    		 |		.	       |
|		.		|			.		|	       .		  |	   .	    		 |		.	       |
+---------------+-------------------+---------------------+----------------------+-----------------+*/
-- Type your code below:


SELECT * FROM ratings;
SELECT * FROM names;
SELECT * FROM genre;
SELECT * FROM role_mapping;


WITH actress_sum AS 
(
SELECT n.name AS actress_name, SUM(r.total_votes) AS movie_votes, ROUND( SUM( avg_rating * total_votes ) / SUM( total_votes ), 2 ) AS actress_rating
FROM movie m 
INNER JOIN ratings r 
ON m.id = r.movie_id
INNER JOIN  genre g
ON r.movie_id = g.movie_id
INNER JOIN role_mapping ro
ON g.movie_id = ro.movie_id
INNER JOIN names n 
ON ro.name_id = n.id
WHERE (avg_rating > 8) AND category = "actress" AND genre = "Drama"
GROUP BY actress_name
ORDER BY actress_rating
)
SELECT *,
ROW_NUMBER() OVER( ORDER BY actress_rating DESC ) AS actress_rank
FROM actress_sum;

-- Sangeetha Bhat : 9.60
-- Fatmire Sahiti : 9.40
-- Adriana Matoshi : 9.40











/* Q29. Get the following details for top 9 directors (based on number of movies)
Director id
Name
Number of movies
Average inter movie duration in days
Average movie ratings
Total votes
Min rating
Max rating
total movie durations

Format:
+---------------+-------------------+---------------------+----------------------+--------------+--------------+------------+------------+----------------+
| director_id	|	director_name	|	number_of_movies  |	avg_inter_movie_days |	avg_rating	| total_votes  | min_rating	| max_rating | total_duration |
+---------------+-------------------+---------------------+----------------------+--------------+--------------+------------+------------+----------------+
|nm1777967		|	A.L. Vijay		|			5		  |	       177			 |	   5.65	    |	1754	   |	3.7		|	6.9		 |		613		  |
|	.			|		.			|			.		  |	       .			 |	   .	    |	.		   |	.		|	.		 |		.		  |
|	.			|		.			|			.		  |	       .			 |	   .	    |	.		   |	.		|	.		 |		.		  |
|	.			|		.			|			.		  |	       .			 |	   .	    |	.		   |	.		|	.		 |		.		  |
|	.			|		.			|			.		  |	       .			 |	   .	    |	.		   |	.		|	.		 |		.		  |
|	.			|		.			|			.		  |	       .			 |	   .	    |	.		   |	.		|	.		 |		.		  |
|	.			|		.			|			.		  |	       .			 |	   .	    |	.		   |	.		|	.		 |		.		  |
|	.			|		.			|			.		  |	       .			 |	   .	    |	.		   |	.		|	.		 |		.		  |
|	.			|		.			|			.		  |	       .			 |	   .	    |	.		   |	.		|	.		 |		.		  |
+---------------+-------------------+---------------------+----------------------+--------------+--------------+------------+------------+----------------+

--------------------------------------------------------------------------------------------*/
 -- duration / (60 * 24): Converts the duration in minutes to days.
-- AVG(duration / (60 * 24)): Calculates the average of these differences, giving the average inter-movie duration in days.

-- Type you code below:


SELECT * FROM director_mapping;
SELECT * FROM ratings;
SELECT * FROM names;
SELECT * FROM movie;

WITH direction AS
(
SELECT d.name_id AS director_id, n.name AS director_name, COUNT(d.movie_id) AS number_of_movies, r.avg_rating,
 MIN(avg_rating) AS min_rating, MAX(avg_rating) AS max_avg_rating
FROM director_mapping d 
INNER JOIN names n 
ON d.name_id = n.id
INNER JOIN movie m 
ON d.movie_id = m.id
INNER JOIN ratings r
ON r.movie_id = m.id
)
SELECT *,
ROUND(AVG(m.duration / (60 * 24))) AS avg_inter_movie_days
FROM direction
GROUP BY director_id
ORDER BY number_of_movies DESC LIMIT 10;

