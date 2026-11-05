CREATE TABLE [dbo].[Ally_test] (
    [c1]  VARCHAR (1024) NULL,
    [c2]  VARCHAR (1024) NULL,
    [c3]  VARCHAR (1024) NULL,
    [c4]  VARCHAR (1024) NULL,
    [c5]  VARCHAR (1024) NULL,
    [c6]  VARCHAR (1024) NULL,
    [c7]  VARCHAR (1024) NULL,
    [c8]  VARCHAR (1024) NULL,
    [c9]  VARCHAR (1024) NULL,
    [c10] VARCHAR (1024) NULL,
    [c11] VARCHAR (2000) NULL
);


GO
CREATE NONCLUSTERED INDEX [ix_c2]
    ON [dbo].[Ally_test]([c1] ASC, [c2] ASC);


GO
CREATE NONCLUSTERED INDEX [ix_c11]
    ON [dbo].[Ally_test]([c11] ASC);


GO
CREATE NONCLUSTERED INDEX [ix_c1]
    ON [dbo].[Ally_test]([c1] ASC);

