
print("Python Calculator")

def calculator():

    def sum(x,y):
        return x+y

    def substract(x,y):
        return x-y

    def multiply(x,y):
        return x*y

    def divide(x,y):
        return x/y

    print("Selecione o número da operação desejada: \n")
    print("1 - Soma: \n")
    print("2 - Subtração: \n")
    print("3 - Multiplicação: \n")
    print("4 - Divisão: \n")

    opcao = int(input("Digite sua opção: \n"))

    primeiro = int(input("Digite seu primeiro número: "))
    segundo = int(input("Digite seu segundo número: "))


    if opcao ==1:
        print("\n")
        print(primeiro, "+", segundo, "=",sum(primeiro,segundo))

    elif opcao ==2:
        print("\n")
        print(primeiro, "-",segundo,"=",(substract(primeiro,segundo)))

    elif opcao ==3:
        print("\n")
        print(primeiro,"*",segundo,"=",(multiply(primeiro,segundo)))

    elif opcao==4:
        print("\n")
        print(primeiro,"/",segundo,"=",divide(primeiro,segundo))

    else:
        print("Operação não encontrada")


while True:
    try:
        result = calculator()
        print("Resultado:", result)
        break
    except ValueError as e:
        print("Ocorreu um erro:", e)
    except Exception as e:
        print("Ocorreu um erro desconhecido:", e)