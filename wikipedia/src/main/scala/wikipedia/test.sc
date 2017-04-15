val langs = List("Java", "Scala", "Erlang")

val articles = List(
  "Groovy is pretty interesting, and so is Erlang",
  "Scala and Java run on the JVM",
  "Scala is not purely functional"
)

langs.map(l => (l, articles.filter(_.contains(l))))


val ix = List(
  ("Java", List(
    "Scala and Java run on the JVM"
  )),
  ("Scala", List(
    "Scala and Java run on the JVM",
    "Scala is not purely functional"
  )),
  ("Erlang", List(
    "Groovy is pretty interesting, and so is Erlang"
  ))
)

ix.map(pair => pair._2.map(art => (pair._1, art))).flatten

ix.flatMap(pair => pair._2.map(art => (pair._1, art)))