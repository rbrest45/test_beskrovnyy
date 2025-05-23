package stepic_final

object StringProcessor {

//Убрал ненужный метод

  def main(args: Array[String]): Unit = {
    val strings = List("apple", "cat", "banana", "dog", "elephant")
    val processedStrings = strings.filter(x => x.length > 3).map(x => x.toUpperCase()) // применил к объекту List две функции
    println(s"Processed strings: $processedStrings")
  }

}
