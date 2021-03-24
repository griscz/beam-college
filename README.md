<!-- ------------------------ -->
## Generate HTML

We will use claat (Codelabs as a Thing) to generate HTML files from our Markdown file.
To avoid installing `Go`, I made a light docker image to use claat.


First install the image:
```docker
docker pull bpetetot/claat
```

Change directory to target codelab e.g.: 

```bash
cd beam-college-dataflow
```

Execute claat:
```docker
docker container run -it -v $(pwd):/app bpetetot/claat export codelab.md
```

**You now have generate the codelab as HTML.**

When you are working on the codelab, you can serve it on localhost:
```docker
docker container run -it -p 9090:9090 -v $(pwd):/app bpetetot/claat serve -addr 0.0.0.0:9090 codelab.md
```