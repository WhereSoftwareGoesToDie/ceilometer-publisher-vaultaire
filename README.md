ceilometer-publisher-vaultaire
==============================

A publisher plugin for [Vaultaire].

[Vaultaire]: https://github.com/anchor/vaultaire


How it works
------------

This plugin queues samples and source dictionaries to a local [Marquise] spool.
You will need [Pymarquise] and [libmarquise] installed as dependencies.

[Marquise]: https://github.com/anchor/marquise
[Pymarquise]: https://github.com/anchor/pymarquise
[libmarquise]: https://github.com/anchor/libmarquise


Installation
---

`git clone https://github.com/anchor/libmarquise.git`
cd libmarquise
> follow README install docs

`git clone https://github.com/anchor/pymarquise.git`


cd pymarquise
(sudo) python setup.py install

> if ceilometer user can't access /var/spool:
>   mkdir /var/spool/marquise,
>   chown user:user /var/spool/marquise

git clone https://github.com/anchor/ceilometer-publisher-vaultaire.git
(sudo) python setup.py install

> THEN

> Add vaultaire to part of your pipeline.yaml sink, like:

publishers:
   - vaultaire://namespace
