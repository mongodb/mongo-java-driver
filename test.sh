
ant && javac -cp build/main foo.java && java -cp build/main:build/test:. com.mongodb.$1Test
