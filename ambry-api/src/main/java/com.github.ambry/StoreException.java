package com.github.ambry;


public class StoreException extends Exception
{
    private static final long serialVersionUID = 1;

    public StoreException(String message)
    {
        super(message);
    }

    public StoreException(String message, Throwable e)
    {
        super(message,e);
    }

    public StoreException(Throwable e)
    {
        super(e);
    }
}
