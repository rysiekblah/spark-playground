val langs = List("Java", "Go")
val artic = List("Java is ok", "Java souds good as Go", "Go great as Java")

val st = "Java souds good as Go, Java"
st.split(' ').distinct

artic.flatMap(a => a.distinct)

val l = langs.map(lang => (lang, artic.filter(_.contains(lang))))
l.flatMap(pair => pair._2.map(art => (pair._1, art)))