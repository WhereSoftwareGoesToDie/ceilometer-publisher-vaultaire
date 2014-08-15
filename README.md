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


Clone all the libraries into a location on the server

Install marquise, and libmarquise as per their installation instructions

run `(sudo) python setup.py install` for both pymarquise and ceilometer-publisher-vaultaire

Confirm that the user running ceilometer can write to `/var/spool`. If not:  

```
mkdir /var/spool/marquise
chown user:user /var/spool/marquise
```


Once this is all setup, add the following to the sinks of choice into your `/etc/ceilometer/pipeline.yaml`

```
    publishers:
        - vaultaire://namespace
```

Depending on your setup, you will need to restart: 

`ceilometer-anotification`
`ceilometer-collector`
`ceilometer-acompute`
