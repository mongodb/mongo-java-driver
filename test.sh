
ant && javac -cp build/main foo.java && java -cp build/main:build/test:. $1Test
