# Subscribing Basics


[//]: # (Explicar o que é uma inscrição como feito na introdução do PubSub.)


## The Default Subscription (Pull)


[//]: # (Não é necessário fazer os exemples docs/learn/tutoral/00.index.md novamente)


[//]: # (Porém é necessário fazer a explicação da estrutura de uma inscrição. Em outras palavras, mostrar o código, assinatura do método subscribe, mas não precisa ser um código "executavel")

[//]: # (Explicar que ela possui definições padrões que são razoáveis e que são baseadas no padrão da SDK da google.)

[//]: # (A linha da tabela deve ser a configuração (e.g., max_message) e a coluna a descrição da funcionalidade)

## Subscription Configurations

[//]: # (Citar que existem multiplas configurações diferentes que podem ser feitas para a inscrição. Elas podem ser dividas em três tipos: configurações de inscrição, configurações de controle (autocreate/update) e configurações do handler (middlewares). Criar uma tabela que mostra quais dessas configurações são só aplicaveis no momento da criação ou podem ser atualizadas. Incluir configurações que podem ser alteradas a qualquer momento.)




## FastPubSub and Push Subscription

[//]: # (Citar as inscrições do tipo push. Dizer que o FastPubSub não cobre sua criação. Porém é possível receber elas via API, mas não conseguimos criar ela.)

[//]: # (Aqui sim devemos ter um exemplo de endpoint que recebe mensagens PubSub pra isso)



## FastPubSub and Async Programming

[//]: # (Precisamos informar que o FastPubSub é integrado com asyncio via anyio)

[//]: # (Diferentemente do FastAPI nosso processo é obrigatório ser asincrono. O motivo é que a thread do processo que recebe requisições é a mesma que processa e consome mensagems. Então todo processo de consumo de dados precisa ser não-bloqueante para que o loop de eventos pode alterar a execução de tarefas. Por exemplo, se uma requisição de API REST chegar no mesmo momento que uma mensagem está sendo processada.)



[//]: # (Podemos ter também um diagrama de sequencia feito em mermeid. Dois na verdade, um deles mostra uma aplicação FastPubSub que tem um endpoint e subscriber. Nesse diagrama durante o processo de computação da mensagem o subscriber faz uma operação bloqueante, por exemplo sleep, ai a API Rest seria incapaz de responder pois o processo está esperando o sleep acabar. No segundo diagrama podemos ter o mesmo exemplo, mas ao invés do sleep, o asyncio.sleep é usado. Por ser uma operação não bloqueante a API Rest ainda consegue responder)


## Recap



[//]: # (Recapitular todos os principais pontos acima.)
