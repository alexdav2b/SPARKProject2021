def piece(x: Int): Int = {
  if (x >= 500){
    return 500
  }
  else if(x >= 200) {

    return 200
  }
  else if(x >= 100) {

    return 100
  }
  else if(x >= 50) {

    return 50
  }
  else if(x >= 20) {

    return 20
  }
  else if(x >= 10) {

    return 10
  }
  else if(x >= 5) {

    return 5
  }
  else if(x >= 2) {

    return 2
  }
  else if(x >= 1) {

    return 1
  }
  0
}

var valeur = 542

var result = (valeur + " euros => ")

while(valeur > 0){
  val tmp = piece(valeur)
  valeur = valeur - tmp
  result = result + tmp + ", "
}
print(result.substring(0, result.length - 2))