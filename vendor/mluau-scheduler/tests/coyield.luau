function myCoroutine()
    print("Hello, Codecademy!")
    coroutine.yield() -- Should yield
    print("How are you?")
  end
  
  co = coroutine.create(myCoroutine)
  task.wait(2)
  print(coroutine.status(co))
  coroutine.resume(co)
  task.wait(10)
  coroutine.resume(co)
  print("End of program")