package imdb

class ImdbLib {

  def intervalle(value: Int) : Int = {
    value match {
      case x if 1000000 until 1000000000 contains x => return 4
      case x if 100941 until 1000000 contains x => return 3
      case x if 10000 until 100941 contains x => return 2
      case x if 1000 until 10000 contains x => return 1
      case x if (value < 1000  ) => return 0
    }
  }
}