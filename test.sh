
ant && javac -cp build/main foo.java && java -cp build/main:build/test:. -Xmx256m $1Test
