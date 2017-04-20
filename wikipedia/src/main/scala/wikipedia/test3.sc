import wikipedia.{WikipediaArticle}
val langs = List("Java", "Groovy", "Scala")
val articles = List(
  WikipediaArticle("1","Groovy is pretty interesting, and so is Erlang"),
  WikipediaArticle("2","Scala and Java run on the JVM"),
  WikipediaArticle("3","Scala is not purely functional"),
  WikipediaArticle("4","The cool kids like Haskell more than Java"),
  WikipediaArticle("5","Java is for enterprise developers")
)

articles.flatMap(w =>
    w.text.split("\\s+").map((_, w))
)