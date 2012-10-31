"""
The SubTP protocol defines a way of broadcasting data changes over the wire.

This module allows your application to register Django models and receive
representations of CRUD operations back through a callback interface.

Example usage:

    import subtp

    class MyModel(django.db.models.Model):
        # etc...
        pass
    
    def my_callback(model_class, event, pk, attributes):
        # etc...
        pass

    subtp.register(MyModel)
    subtp.add_callback(my_callback)

It is implemented by listening to the Django signals ``post_init``,
``post_save``, and ``post_delete``.
"""

from django.db.models.signals import post_save, post_delete, post_init
from django.dispatch import receiver

import logging
import weakref

logger = logging.getLogger(__name__)

_snapshots = dict()
_callbacks = []
_registered_models = []


def register(model_class):
    """
    Register a Django model for tailing.
    """
    global _registered_models
    logger.debug("Registering model: %s..." % model_class)
    _registered_models.append(model_class)


def unregister(model_class):
    """
    Unregister a Django model.
    """
    global _registered_models
    logger.debug("Unregistering model: %s..." % model_class)
    _registered_models.remove(model_class)


def add_callback(callback):
    """
    Register a callback which will be invoked after any change is
    written to the database using Django's ORM. The callback should expect
    four keyword arguments: ``model_class``, which is the Django model
    changed, ``event``, which is one of "create", "update", or "delete",
    ``pk``, which is the object's primary key, and optionally
    ``attributes``, which is a representation of the attributes changed,
    if any.
    """
    global _callbacks
    logger.debug("Adding to callbacks: %s..." % callback)
    _callbacks.append(callback)


def publish(model_class, event, pk, attributes=None):
    """
    Actually invoke all SubTP callbacks with the correct keyword arguments.
    For a description of the arguments, see the docstring for
    add_callback().
    """
    global _callbacks
    logger.debug("publish("
        "model_class=%(model_class)s, "
        "event=%(event)s, "
        "pk=%(pk)s, "
        "attributes=%(attributes)s"
        ")" % 
        dict(model_class=model_class,
             event=event,
             pk=pk,
             attributes=attributes
            )
    )

    payload = {
        "id": pk,
    }
    if attributes:
        payload.update({
            "data": attributes,
        })
    for callback in _callbacks:
        callback(
            model_class=model_class,
            event=event,
            payload=payload,
        )


@receiver(post_init, dispatch_uid="subtp.post_init_callback")
def post_init_callback(sender, **kwargs):
    """
    Invoked after every model object's __init__(), this method provides
    special behavior for Django models registered with the register()
    function. It saves a key-value mapped snapshot of every object's
    initial state, indexed by the original object's memory address. It
    also registers a destructor which is called to delete the snapshot
    when the garbage collector deletes the object.
    """
    global _registered_models
    if sender not in _registered_models:
        return
    logger.debug("post_init_callback(%s)" % sender)

    instance = kwargs['instance']
    logger.debug("-- instance = %s" % instance.__dict__)

    # Avoid a memory leak by registering a destructor callback to clean up
    # _snapshots for dead objects.
    # NOTE: I don't think this is being invoked, but I don't know why.
    def destructor(reference):
        logger.debug("destructor called for %s" % reference)
        if id(instance) in _snapshots:
            logger.debug("Destroying snapshot %s..." % id(instance))
            del _snapshots[id(instance)]
            logger.debug(
                "Destructor deleted id=%s from _snapshots." %
                id(instance)
            )
    weakref.ref(instance, destructor)

    attributes = get_attributes(instance)

    _snapshots[id(instance)] = attributes
    logger.debug("-- set snapshot %s -> %s" % (id(instance), attributes))


def get_attributes(instance):
    whitelist =  [field.name for field in instance._meta.fields]
    return {
        key: value
        for (key, value)
        in instance.__dict__.items()
        if key in whitelist
    }

@receiver(post_save, dispatch_uid="subtp.post_save_callback")
def post_save_callback(sender, **kwargs):
    """
    Invoked at the end of after every model object's save(), this
    method broadcasts a "create" event if the object is newly created
    or an "update" event if the object is updated, taking a diff
    between the last snapshot and the most recent values. After the
    broadcast, it saves a new snapshot.
    """
    global _registered_models
    if sender not in _registered_models:
        return
    logger.debug("post_save_callback(%s)" % sender)

    instance = kwargs["instance"]
    logger.debug("-- instance = %s" % instance.__dict__)

    attributes = get_attributes(instance)

    if kwargs.get("created"):
        logger.debug("Django reports object created.")
        logger.debug("-- attributes=%s" % attributes)
        publish(sender, "create", instance.pk, attributes)
    else:
        logger.debug("Django reports object not created.")
        logger.debug("-- attributes=%s" % attributes)
        previous = _snapshots.get(id(instance), None)
        if not previous:
            raise KeyError(
                "Previous snapshot not found when expected, with "
                "instance=%s, "
                "attributes=%s" %
                dict(
                    instance=instance,
                    attributes=attributes,
                )
            )
        logger.debug("-- got snapshot %s -> %s" % (id(instance), previous))

        delta = {
            key: attributes[key]
            for key in attributes
            if attributes[key] != previous[key]
        }
        logger.debug("-- delta is %s" % delta)
        publish(sender, "update", instance.pk, delta)

    _snapshots[id(instance)] = attributes


@receiver(post_delete, dispatch_uid="subtp.post_delete_callback")
def post_delete_callback(sender, **kwargs):
    """
    Invoked after data has been deleted from the database, this method
    broadcasts a "delete" event.
    """
    global _registered_models
    if sender not in _registered_models:
        return
    logger.debug("post_delete_callback(%s)" % sender)
    instance = kwargs["instance"]
    logger.debug("-- instance = %s" % instance)
    publish(sender, "delete", instance.pk)
