class NSKG(scale: Int, ratio: Int, noise: Double, a: Double, b: Double, c: Double, d: Double) extends SKG(scale: Int, ratio: Int, a: Double, b: Double, c: Double, d: Double) {
  @inline val c_ad: Double = 2 / (a + d)
  @inline val c_c: Double = 1 / c
  @inline val c_adadab: Double = (a - d) / ((a + d) * (a + b))
  @inline val c_adadcd: Double = (a - d) / ((a + d) * (c + d))

  @inline val kmu: Array[Double] = Array.tabulate(scale)(x => -noise + 2 * noise * math.random)

  @inline final def getPoutWithoutNoise(vertexId: Long): Double = super.getPout(vertexId)

  @inline def getCDFWithoutNoise(vertexId: Long, logTo: Int): Double = super.getCDF(vertexId, logTo)

  @inline val len: Int = math.ceil(scale / 16d).toInt

  @inline val c_pre1: Array[Array[Double]] = Array.tabulate(len, 1 << 16)({ (offset, vertexId) =>
    var tab = 1d
    var x = offset * 16
    while (x < math.min((offset + 1) * 16, scale)) {
      val t = if ((vertexId >>> (offset * 16) >>> x) % 2 == 0) 1d - c_adadab * kmu(x) else 1d + c_adadcd * kmu(x)
      tab *= t
      x += 1
    }
    tab
  })

  @inline override def getPout(vertexId: Long): Double = {
    var tab = 1d
    var shift = 0
    while (shift < len) {
      tab *= c_pre1(shift)((vertexId << (48 - shift) >>> 48).toInt)
      shift += 16
    }
    getPoutWithoutNoise(vertexId) * tab
  }

  @inline val c_pre2: Array[Array[Array[Double]]] = Array.tabulate(len, scale + 1, 1 << 16)({ (offset, logTo, vertexId) =>
    var tab = 1d
    var x = offset * 16
    while (x < math.min((offset + 1) * 16, scale)) {
      val t = if (x < scale - logTo) {
        if ((vertexId >>> (offset * 16) >>> x) % 2 == 0) 1d - c_ad * kmu(x) else 1d + c_c * kmu(x)
      } else {
        if ((vertexId >>> (offset * 16) >>> x) % 2 == 0) 1d - c_adadab * kmu(x) else 1d + c_adadcd * kmu(x)
      }
      tab *= t
      x += 1
    }
    tab
  })

  @inline override def getCDF(vertexId: Long, logTo: Int): Double = {
    var tab = 1d
    var shift = 0
    while (shift < len) {
      tab *= c_pre2(shift)(logTo)((vertexId << (48 - shift) >>> 48).toInt)
      shift += 16
    }
    getCDFWithoutNoise(vertexId, logTo) * tab
  }
}