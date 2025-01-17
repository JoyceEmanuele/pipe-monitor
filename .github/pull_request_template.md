# Descrição

<!--- Descreva suas mudanças -->

<!--- Código da Issue -->
Fix Issue: #

<!--- Se não for necessário adicionar Screenshots pode deletar essa seção -->
## Screenshots

<!--- Go over all the following points, and put an `x` in all the boxes that apply. -->
## Checklist

### Checklist do Desenvolvedor

- [ ] Revisei meu próprio commit nesse PR
- [ ] Documentei no sistema de gerenciamento de tarefas (ClickUp) as mudanças implementadas
- [ ] Implementei testes para cobrir minhas mudanças
- [ ] Ajustei os testes unitários das alterações realizadas
- [ ] Testei quaisquer migrações do banco de dados (com diesel migration redo)
- [ ] Documentei as novas APIs (se alguma foi criada) com o padrão do OpenAPI (Swagger)
- [ ] Verifiquei e corrigi todas as Issues no SonarQube
- [ ] Verifiquei no Actions que o pipeline rodou sem apresentar nenhum problema
- [ ] Resolvi todos os conflitos

### Checklist do Code Reviewer 1

- [ ] O desenvolvedor marcou OK em toda sua checklist
- [ ] O pipeline do PR rodou sem problemas (Testes Unitários e SonarQube)
- [ ] Novas APIs foram documentadas
- [ ] APÌs modificadas teve sua documentação atualizada
- [ ] Testes unitários foram criados para novas funções
- [ ] Testes unitários foram atualizados para funções modificadas
- [ ] Identificar pontos no sistema que essa mudança pode afetar
- [ ] Erros foram tratados e possibilidades de panic foram justificadas em comentário
- [ ] Foram adicionados log nos locais corretos e com mensagens claras
- [ ] Os comandos SQL estão corretos e otimizados

### Checklist do Code Reviewer 2
- [ ] O desenvolvedor marcou OK em toda sua checklist
- [ ] O pipeline do PR rodou sem problemas (Testes Unitários e SonarQube)
- [ ] Novas APIs foram documentadas
- [ ] APÌs modificadas teve sua documentação atualizada
- [ ] Testes unitários foram criados para novas funções
- [ ] Testes unitários foram atualizados para funções modificadas
- [ ] Identificar pontos no sistema que essa mudança pode afetar
- [ ] Erros foram tratados e possibilidades de panic foram justificadas em comentário
- [ ] Foram adicionados log nos locais corretos e com mensagens claras
- [ ] Os comandos SQL estão corretos e otimizados