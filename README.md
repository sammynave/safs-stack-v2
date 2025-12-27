# Plan

1. implement sqlite interfaces
  1. memory
    - sync
  2. opfs
    - event based to free up the UI as quick as possible.
    - no `async/await`.
2. implement syncing protocol
3. syncing end-to-end
4. ivm cache layer - instant writes and UI updates


previous:
- https://github.com/sammynave/safs-stack-v1
  - db interface
  - ivm
  - bad version of sync
- https://github.com/sammynave/habits
  - old cr-sql repo
  - good version of sync + sync server to take from
- https://github.com/sammynave/safs-stack-v0
  - old, nothing to port here
- https://github.com/sammynave/safs-sync
  - old, nothing to port here
- https://github.com/sammynave/workspace
  - whiteboard library
- https://github.com/sammynave/forseti
  - first version of ivm
- https://github.com/safs-io/unify
  - datalog attempt
  - might be some good stuff to pull from if we opt for a custom made, non-sqlite db

# Svelte library

Everything you need to build a Svelte library, powered by [`sv`](https://npmjs.com/package/sv).

Read more about creating a library [in the docs](https://svelte.dev/docs/kit/packaging).

## Creating a project

If you're seeing this, you've probably already done this step. Congrats!

```sh
# create a new project in the current directory
npx sv create

# create a new project in my-app
npx sv create my-app
```

## Developing

Once you've created a project and installed dependencies with `npm install` (or `pnpm install` or `yarn`), start a development server:

```sh
npm run dev

# or start the server and open the app in a new browser tab
npm run dev -- --open
```

Everything inside `src/lib` is part of your library, everything inside `src/routes` can be used as a showcase or preview app.

## Building

To build your library:

```sh
npm pack
```

To create a production version of your showcase app:

```sh
npm run build
```

You can preview the production build with `npm run preview`.

> To deploy your app, you may need to install an [adapter](https://svelte.dev/docs/kit/adapters) for your target environment.

## Publishing

Go into the `package.json` and give your package the desired name through the `"name"` option. Also consider adding a `"license"` field and point it to a `LICENSE` file which you can create from a template (one popular option is the [MIT license](https://opensource.org/license/mit/)).

To publish your library to [npm](https://www.npmjs.com):

```sh
npm publish
```
