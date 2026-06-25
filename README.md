# PrismaFlow — Documentação

Documentação oficial do PrismaFlow, construída com [Mintlify](https://mintlify.com).
Estrutura em MDX + `docs.json`. O Mintlify faz o build e a hospedagem — você não gerencia servidor.

## Estrutura

```
prismaflow-docs/
├─ docs.json              # configuração: nome, cores, navegação, navbar, footer
├─ favicon.svg            # ícone do site
├─ logo/
│  ├─ light.svg           # logo p/ tema claro
│  └─ dark.svg            # logo p/ tema escuro
├─ index.mdx              # Introdução
└─ eventos/               # Eventos
   ├─ definicoes.mdx
   ├─ computed-traits.mdx
   ├─ identidades.mdx
   └─ quarentena.mdx
```

Navegação: uma única tab **"Comece aqui"** → grupo **Tópicos** (Introdução + Eventos).

## Rodar localmente

Pré-requisito: Node.js 18+.

```bash
# instala a CLI do Mintlify (uma vez)
npm i -g mint

# na raiz do projeto (onde está o docs.json)
mint dev
```

Abre em `http://localhost:3000`. O preview recarrega ao salvar.

> Se aparecer aviso de versão antiga: `mint update`.

## Publicar (hospedagem grátis no plano Hobby)

1. Suba esta pasta como um repositório no GitHub.
2. Crie a conta em [mintlify.com](https://mintlify.com) e instale o **GitHub App** do Mintlify no repo.
3. A cada `push` na branch principal, o Mintlify faz build e deploy automático.
4. Domínio próprio (ex.: `docs.prismaflow.com`) está incluído até no plano grátis.

## AI-friendly (de graça)

O Mintlify gera e hospeda automaticamente:

- `/llms.txt` — índice limpo de todas as páginas, pra IAs descobrirem o conteúdo.
- `/llms-full.txt` — toda a doc num único arquivo de texto, pra alimentar LLMs.

Não precisa configurar nada — basta manter o conteúdo em MDX bem estruturado.
Cada página também responde em Markdown puro adicionando `.md` na URL.

## Próximos passos

Vamos preencher tema por tema (ver páginas marcadas como "em construção"):
Introdução → Eventos: Definições → Computed traits → Identidades → Quarentena.
