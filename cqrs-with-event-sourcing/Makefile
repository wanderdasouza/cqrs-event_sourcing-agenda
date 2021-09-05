all:
	cd .. && docker-compose up -d --build
	cd agenda-read && sbt run
	cd agenda-write && sbt run 
